package repo_cassandra

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/plykit/hajobs"
	"log"
	"time"
)

type lock struct {
	r       *LockRepo
	name    string
	owner   string
	session *gocql.Session
	hbctx   context.Context
	cancel  context.CancelFunc
}

// A simple in memory lock repository
type LockRepo struct {
	session   *gocql.Session
	traceflag bool
}

// verify the client implements the jobs repo for the job manager
var _ = hajobs.LockRepo(&LockRepo{})

type data struct {
	Name    string
	Owner   string
	Ttl     time.Duration
	Expires time.Time
	Version int8
}

func newdata(name string, owner string, now time.Time, ttl time.Duration) data {
	return data{
		Name:    name,
		Owner:   owner,
		Ttl:     ttl,
		Expires: now.Add(ttl),
		Version: 0,
	}
}

func nextdata(d data) data {
	return data{
		Name:    d.Name,
		Owner:   d.Owner,
		Ttl:     d.Ttl,
		Expires: time.Now().UTC().Add(d.Ttl),
		Version: d.Version + 1,
	}
}

func NewLockRepo(session *gocql.Session) *LockRepo {
	return &LockRepo{session: session, traceflag: false}
}

const jobLock = "JobLock"

// Try to acquire the referenced lock and if successful, start the heartbeat
// on the log to refresh its expiry time.
//
// Lock() returns three values, applied, error, error channel
//   - applied: return sfals if the lock could not be akquired
//   - error: if during akquire operation an error occurred, it is returned here, nil otheriwse
//   - error channel: if an error occurs during heartbeat, the heartbeat is stopped and the error
//     is sent on this channel. It is extremely important to observe this channel because if
//     heartbeat stops, the lock will expire while the working process assumes it still has it. This
//     will lead to concurrent runs of the job.
//
// The heartbeat will be stopped if the provided context is canceled.
func (r *LockRepo) Lock(ctx context.Context, name string, owner string, ttl time.Duration) (hajobs.Lock, error, <-chan error) {
	r.trace("TRY LOCK %s with ttl %d", name, int(ttl.Seconds()))

	now := time.Now().UTC()
	initialData := newdata(name, owner, now, ttl)
	applied, err := r.lock(ctx, now, initialData)
	if err != nil {
		return nil, err, nil
	}
	if !applied {
		return nil, nil, nil
	}

	errChan := make(chan error)
	hbctx, cancel := context.WithCancel(context.Background())
	lock := &lock{
		r:       r,
		name:    name,
		owner:   owner,
		session: r.session,
		hbctx:   hbctx,
		cancel:  cancel,
	}

	go func() {
		nextData := nextdata(initialData)
		heartbeatInterval := ttl / 2
		r.trace("Heartbeat interval: %d", int(heartbeatInterval.Seconds()))
		signalTouchLock := time.After(heartbeatInterval)

		for {
			select {
			case <-lock.hbctx.Done():
				r.trace("Cancel received, stopping heartbeat on lock %s", name)
				return
			case <-signalTouchLock:
				nextData, err = r.touch(lock.hbctx, nextData)
				if err != nil {
					errChan <- fmt.Errorf("lock ttl update failed for %s (owner: %s): %w. Stopping heartbeat", name, owner, err)
					return
				}
				signalTouchLock = time.After(heartbeatInterval)
			}
		}
	}()
	return lock, nil, errChan
}

// Try to akquire the lock and set the initial expiry to now+ttl.
// returns true if the lock was akquired.
func (r *LockRepo) lock(ctx context.Context, now time.Time, d data) (bool, error) {
	var existingLock data

	query := "select name, owner, expires from job_lock WHERE name = ? LIMIT 1"
	iter := r.session.Query(query, d.Name).WithContext(ctx).Iter()
	if iter.Scan(&existingLock.Name, &existingLock.Owner, &existingLock.Expires) {
		r.trace("LOCK %s exists, it expires in %d secs", d.Name, int(existingLock.Expires.Sub(now).Seconds()))

		if existingLock.Expires.After(now) {
			r.trace("Lock %s held by %s, it expires in %d sec (at %s)", existingLock.Name, existingLock.Owner, int(existingLock.Expires.Sub(now).Seconds()), existingLock.Expires.String())
			return false, nil
		}

		r.trace("Lock %s held by %s, but is expired", existingLock.Name, existingLock.Owner)
		updateQuery := "UPDATE job_lock SET owner = ?, expires = ? WHERE name = ? IF expires <= ?"
		applied, err := r.session.Query(updateQuery, d.Owner, d.Expires, d.Name, now).WithContext(ctx).ScanCAS()
		if err != nil {
			return false, fmt.Errorf("failed to update expired lock %s: %w", d.Name, err)
		}
		if !applied {
			r.trace("Lock %s not acquired by %s (concurrent update)", d.Name, d.Owner)
			return false, nil
		}
	} else {
		insertQuery := "INSERT INTO job_lock (name, owner, expires) VALUES (?, ?, ?) IF NOT EXISTS"
		applied, err := r.session.Query(insertQuery, d.Name, d.Owner, d.Expires).WithContext(ctx).ScanCAS()
		if err != nil {
			return false, fmt.Errorf("failed to create lock %s: %w", d.Name, err)
		}
		if !applied {
			r.trace("Lock %s already acquired by another owner", d.Name)
			return false, nil
		}
	}

	r.trace("Lock %s acquired by %s", d.Name, d.Owner)
	return true, nil
}

// Update the locks expiry time using the given TTL.
func (r *LockRepo) touch(ctx context.Context, d data) (data, error) {
	newExpiration := time.Now().UTC().Add(d.Ttl)

	query := "UPDATE job_lock SET owner = ?, expires = ? WHERE name = ? IF expires <= ?"
	applied, err := r.session.Query(query, d.Owner, newExpiration, d.Name, newExpiration).WithContext(ctx).ScanCAS()
	if err != nil {
		return data{}, fmt.Errorf("update lock %s: %w", d.Name, err)
	}
	if !applied {
		return data{}, fmt.Errorf("extend lock %s, possibly lost", d.Name)
	}

	r.trace("lock %s refreshed by %s, new expiry: %s", d.Name, d.Owner, newExpiration)

	return data{Name: d.Name, Owner: d.Owner, Ttl: d.Ttl, Expires: newExpiration}, nil
}

func (lock lock) Unlock(ctx context.Context) error {
	lock.cancel()

	time.Sleep(30 * time.Second)
	deleteCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	lock.r.trace("Unlocking %s", lock.name)

	query := "DELETE from job_lock WHERE name = ?"
	if err := lock.session.Query(query, lock.name).WithContext(deleteCtx).Exec(); err != nil {
		lock.r.trace("UNLOCK ERROR: %v", err)
		return fmt.Errorf("failed to unlock %s: %w", lock.name, err)
	}

	lock.r.trace("Unlocked %s", lock.name)
	return nil
}

func (r *LockRepo) trace(format string, args ...interface{}) {
	if r.traceflag == true {
		s := fmt.Sprintf(format, args...)
		log.Printf("TRACE %s", s)
	}
}
