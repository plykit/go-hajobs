package hajobs

import (
	"fmt"
	"time"

	"github.com/gorhill/cronexpr"
)

// JobInfo metadata
type JobInfo struct {
	Name     string `json:"name"`
	Checksec int    `json:"checksec"`
	Enabled  bool   `json:"enabled"`
	//owner      string
	Lockttlsec  int `json:"lockttlsec"`
	Schedule    *cronexpr.Expression
	Cronexpr    string
	LastUpdated time.Time
	State       []byte
}

// shouldTryLock determines for a given JobInfo and reference time whether
// an execution is pending.
func (li *JobInfo) shouldTryLock(now time.Time) bool {
	// Only Enabled jobs should be run at all.
	if !li.Enabled {
		return false
	}

	// If job's lock is taken already, no execution necessary.
	//if li.owner != "" {
	//	return false
	//}

	// Not run yet? => Let's try to run it!
	if li.LastUpdated.IsZero() {
		return true
	}

	// Next run already in the past? => Let's try to run it!
	next := li.Schedule.Next(li.LastUpdated)
	return next.Before(now)
}

// JobCfg contains data to create a job if not present yet.
type JobCfg struct {
	checkSec   int
	enabled    bool
	lockTtlSec int
	schedule   string
}

func ToJobInfo(name string, c JobCfg) (JobInfo, error) {
	schedule, err := cronexpr.Parse(c.schedule)
	if err != nil {
		return JobInfo{}, fmt.Errorf("parse schedule for job %s schedule %s: %w", name, c.schedule, err)
	}
	return JobInfo{
		Name:       name,
		Checksec:   c.checkSec,
		Enabled:    c.enabled,
		Lockttlsec: c.lockTtlSec,
		Schedule:   schedule,
		Cronexpr:   c.schedule,
	}, nil

}

//func (j JobCfg) CheckSec() int    { return j.checkSec }
//func (j JobCfg) Enabled() bool    { return j.Enabled }
//func (j JobCfg) LockTtlSec() int  { return j.lockTtlSec }
//func (j JobCfg) Schedule() string { return j.Schedule }
