package mem

import (
	"context"
	"fmt"
	"github.com/plykit/go-hajobs"
	"sync"
	"time"
)

type lockRepo struct {
	mutex     sync.Mutex
	executors map[string]*data
}

// verify the client implements the jobs repo for the job manager
var _ = hajobs.LockRepo(&lockRepo{})

func NewLockRepo() *lockRepo {
	return &lockRepo{
		executors: make(map[string]*data),
	}
}

type lock struct {
	r      *lockRepo
	name   string
	owner  string
	hbctx  context.Context
	cancel context.CancelFunc
}

type data struct {
	Name    string
	Owner   string
	Ttl     time.Duration
	Expires time.Time
	Version int8
}

func newdata(id, owner string, now time.Time, ttl time.Duration) data {
	return data{
		Name:    id,
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
		Expires: time.Now().Add(d.Ttl),
		Version: d.Version + 1,
	}
}

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
func (r *lockRepo) Lock(ctx context.Context, name string, owner string, ttl time.Duration) (hajobs.Lock, error, <-chan error) {
	now := time.Now()
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
	lock := lock{
		r:      r,
		name:   name,
		owner:  owner,
		hbctx:  hbctx,
		cancel: cancel,
	}

	go func() {
		nextData := nextdata(initialData)
		heartbeatInterval := time.Duration(ttl.Nanoseconds() / 2)
		signalTouchLock := time.After(heartbeatInterval)
		for {
			select {
			case <-lock.hbctx.Done():
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
func (r *lockRepo) lock(ctx context.Context, now time.Time, d data) (bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	lck, ok := r.executors[d.Name]
	if !ok {
		r.executors[d.Name] = &d
		return true, nil
	}
	if lck.Expires.Before(now) {
		r.executors[d.Name] = &d
		return true, nil
	}
	return false, nil
}

// Update the locks expiry time using the given TTL.
func (r *lockRepo) touch(ctx context.Context, d data) (data, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	lock, ok := r.executors[d.Name]
	if !ok {
		return data{}, fmt.Errorf("no data for name %s", d.Name)
	}
	if lock.Owner != d.Owner {
		return data{}, fmt.Errorf("data %s owned by %s, but expected owner %s", d.Name, lock.Owner, d.Owner)
	}
	// Refresh the data expiry with TTL
	d.Expires = time.Now().Add(d.Ttl)
	r.executors[d.Name] = &d
	return nextdata(d), nil
}

func (lock lock) Unlock(ctx context.Context) error {
	lock.cancel()
	lock.r.mutex.Lock()
	delete(lock.r.executors, lock.name)
	defer lock.r.mutex.Unlock()
	return nil
}
