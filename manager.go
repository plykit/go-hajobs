package hajobs

import (
	"context"
	"sync"
	"time"
)

type Manager interface {
	Register(name string, schedule string, run func(ctx context.Context, commitFunc func(state []byte) error, state []byte) ([]byte, error))
	StartAll()
}

type JobsRepo interface {
	GetJobinfo(ctx context.Context, name string) (*JobInfo, error)
	SaveState(ctx context.Context, name string, state []byte) error
	Commit(ctx context.Context, name string, state []byte) error
	CreateJob(ctx context.Context, name string, jobCfg JobCfg) error
}
type Lock interface {
	Unlock(context context.Context) error
}

type LockRepo interface {
	// Lock - name is the lock Name
	// owner is used to indicate the current holder of a lock (eg the hostname)
	// interval is the lock TTL
	// returns lock object that is a handle for unlocking a lock
	Lock(context context.Context, name string, owner string, interval time.Duration) (Lock, error, <-chan error)
}

type manager struct {
	ctx       context.Context
	id        string
	repo      JobsRepo
	lockRepo  LockRepo
	mutex     sync.Mutex
	executors map[string]*executor
}

// NewManager creates a new job manager.
// The provided context will be used to execute the individual job functions.
// In order to cancel all job processing, the provided context can be cancelled.
func NewManager(ctx context.Context, id string, jobsRepo JobsRepo, lockRepo LockRepo) Manager {
	return &manager{
		ctx:       ctx,
		id:        id,
		repo:      jobsRepo,
		lockRepo:  lockRepo,
		executors: make(map[string]*executor),
	}
}

// Register the given job function with a schedule.
// schedule is a standard cron expression of the form
// "*/5 * * * * * *".
func (m *manager) Register(name string, schedule string, f func(context.Context, func(state []byte) error, []byte) ([]byte, error)) {
	jc := JobCfg{
		checkSec:   2,
		lockTtlSec: 10,
		schedule:   schedule,
		enabled:    true,
	}
	ex := &executor{
		ctx:         m.ctx,
		repo:        m.repo,
		lockRepo:    m.lockRepo,
		id:          m.id,
		name:        name,
		jobFunc:     f,
		maxDelaySec: 60,
		//baseDelaySec: 2,
		jobCfg:    jc,
		traceFlag: false,
	}
	m.mutex.Lock()
	m.executors[name] = ex
	m.mutex.Unlock()

}

// StartAll the jobs registered with the manager.
func (m *manager) StartAll() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, v := range m.executors {
		go v.run()
	}
}

// Use this if commit functionality is not supported.
//var noopCommit = func(context.Context, []byte) {}
