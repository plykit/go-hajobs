package hajobs

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"
)

// Internal executor representation.
type executor struct {
	ctx      context.Context
	repo     JobsRepo
	lockRepo LockRepo
	// id is used to propulate the lock's 'owner' field when acquiring the lock.
	id string
	// Name of the job
	name string
	// The function to execute when running the job
	jobFunc func(context.Context, func(state []byte) error, []byte) ([]byte, error)
	// Delays used for initial and retry wait times
	//baseDelaySec int
	maxDelaySec int
	// Filled with current job meta data for every try
	jobInfo *JobInfo
	// Configured JobInfo for creating the job if it does not yet exists
	jobCfg    JobCfg
	traceFlag bool
}

// Recursive function type definition for representing executor as
// State function based State machine.
// See https://www.youtube.com/watch?v=HxaD_trXwRE
type stateFn func(ex *executor) stateFn

func (ex *executor) run() {
	if ex.maxDelaySec <= ex.jobCfg.checkSec {
		// TODO handle better or automatic
		panic("max delay <= checksec")
	}
	for state := checking; state != nil; {
		state = state(ex)
	}
}

func creating(ex *executor) stateFn {
	if err := ex.repo.CreateJob(ex.ctx, ex.name, ex.jobCfg); err != nil {
		ex.log(fmt.Errorf("cannot create entry for the job, so job will not start executing: %w", err))
	}
	return checking
}

func checking(ex *executor) stateFn {
	timer := time.After(time.Duration(ex.jobCfg.checkSec) * time.Second)
	retrycnt := 0
	for {
		select {
		case <-ex.ctx.Done():
			ex.trace("Executor context is Done with %v", ex.ctx.Err())
			return nil
		case <-timer:
			jobinfo, err := ex.repo.GetJobinfo(ex.ctx, ex.name)
			if err != nil {
				ex.log(fmt.Errorf("get jobInfo, so job will not start executing: %s", err.Error()))
				ex.trace("Error while getting jobInfo for executer: %s error: %s", ex.name, err.Error())
				retrycnt++
				d := backoff_delay(ex.maxDelaySec, ex.jobCfg.checkSec, retrycnt)
				timer = time.After(d)
				continue
			}
			retrycnt = 0
			if jobinfo == nil {
				return creating
			}
			// update executor job config with data found in database
			updateExecutorConfig(ex, jobinfo)

			if jobinfo.shouldTryLock(time.Now()) {
				ex.jobInfo = jobinfo
				return lockAndRun
			}
			timer = time.After(time.Duration(jobinfo.Checksec) * time.Second)
		}
	}
}

func updateExecutorConfig(ex *executor, jobinfo *JobInfo) {
	ex.jobCfg.checkSec = jobinfo.Checksec
	ex.jobCfg.lockTtlSec = jobinfo.Lockttlsec
	ex.jobCfg.enabled = jobinfo.Enabled
	ex.jobCfg.schedule = jobinfo.Cronexpr
}

func lockAndRun(ex *executor) stateFn {
	getLockCtx, _ := context.WithTimeout(ex.ctx, time.Millisecond*2000)
	jobCtx, jobCancel := context.WithCancel(ex.ctx)
	lock, err, errT := ex.lockRepo.Lock(getLockCtx, ex.name, ex.id, time.Duration(ex.jobInfo.Lockttlsec)*time.Second)
	if err != nil {
		ex.log(fmt.Errorf("cannot get lock because there was an error during the akquire operation: %w", err))
		ex.trace("lock error: %v", err)
		jobCancel()
		return checking
	}
	if lock == nil {
		ex.trace("Lock not free: %v", err)
		jobCancel()
		return checking
	}

	var commitFunc = func(state []byte) error {
		return ex.repo.Commit(jobCtx, ex.name, state)
	}
	var saveFunc = func(ctx context.Context, state []byte) error {
		return ex.repo.SaveState(ctx, ex.name, state)
	}

	errChanJob := runJob(jobCtx, ex.jobFunc, commitFunc, saveFunc, ex.jobInfo.State)
	select {
	case <-ex.ctx.Done():
		err = lock.Unlock(ex.ctx)
		if err != nil {
			ex.log(fmt.Errorf("executor context is done; tried to unlock but got error: %w. Note: the lock will eventually time out after its TTL", err))
			ex.trace("Unlock of ctx.Done failed with error: %v", err)
		}
		jobCancel()
		return nil
	case err := <-errChanJob:
		jobCancel()
		if err != nil {
			ex.log(fmt.Errorf("running the job function returned an error: %w", err))
		}
		err = lock.Unlock(ex.ctx)
		if err != nil {
			ex.log(fmt.Errorf("job finsihed with error; tried to unlock but got error: %w. Note: the lock will eventually time out after its TTL", err))
		}
		return checking
	case err, _ := <-errT:
		jobCancel()
		if err != nil {
			ex.log(fmt.Errorf("the lock heartbeat signaled an error: %w. Therefore we canceled the job", err))
		}
		return checking
	}
}

func runJob(ctx context.Context, jobFunc func(context.Context, func(state []byte) error, []byte) ([]byte, error), commitFunc func(state []byte) error, saveFunc func(context.Context, []byte) error, state []byte) <-chan error {
	errChan := make(chan error)
	go func() {

		newState, err := jobFunc(ctx, commitFunc, state)
		if err != nil {
			errChan <- err
			return
		}
		errChan <- saveFunc(ctx, newState)
	}()
	return errChan
}

func (ex *executor) log(err error) {
	log.Println(err)
}

func (ex *executor) trace(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	log.Printf("TRACE [%s:%s] %s", ex.name, ex.id, s)
}

// int min function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// backoff_delay calculates exponential backoff interval
func backoff_delay(maxdelaysec int, basedelaysec int, attempt int) time.Duration {
	delay := time.Duration(min(maxdelaysec, basedelaysec+int(math.Pow(2, float64(attempt))))) * time.Second
	jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
	return delay + jitter
}

//func base_delay(basedelaysec int) time.Duration {
//	delay := time.Duration(basedelaysec) * time.Second
//	jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
//	return delay + jitter
//}
