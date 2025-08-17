package repo_mem

import (
	"context"
	"fmt"
	"github.com/plykit/go-hajobs"
	"sync"
	"time"

	"github.com/gorhill/cronexpr"
)

//Schedule   *cronexpr.Expression

// Job metadata
type jinfo struct {
	Name       string
	Checksec   int
	Enabled    bool
	Lockttlsec int
	Schedule   string
	Last       time.Time
	State      []byte
}

func jinfoToJobinfo(in *jinfo) (*hajobs.JobInfo, error) {
	if in == nil {
		return nil, nil
	}

	schedule, err := cronexpr.Parse(in.Schedule)
	if err != nil {
		return nil, fmt.Errorf("cannot parse cron expression %q: %w", in.Schedule, err)
	}

	return &hajobs.JobInfo{
		Name:        in.Name,
		Checksec:    in.Checksec,
		Enabled:     in.Enabled,
		Lockttlsec:  in.Lockttlsec,
		Schedule:    schedule,
		Cronexpr:    in.Schedule,
		LastUpdated: in.Last,
		State:       in.State,
	}, nil
}

func jinfoFromJobinfo(in *hajobs.JobInfo) jinfo {
	if in == nil {
		return jinfo{}
	}

	return jinfo{
		Name:       in.Name,
		Checksec:   in.Checksec,
		Enabled:    in.Enabled,
		Lockttlsec: in.Lockttlsec,
		Schedule:   in.Cronexpr,
		Last:       in.LastUpdated,
		State:      in.State,
	}
}

const (
	jobsInfoKind = "jobsInfo"
)

type jobsRepo struct {
	jobinfos map[string]*jinfo
	mutex    sync.Mutex
}

// verify the client implements the jobs repo for the job manager
var _ = hajobs.JobsRepo(&jobsRepo{})

func NewJobsRepo() *jobsRepo {
	return &jobsRepo{
		jobinfos: make(map[string]*jinfo),
	}
}

// Get the entry for the job with the given name from the repository. If the
// job does not exist return nil,nil
func (r *jobsRepo) get(ctx context.Context, name string) (*jinfo, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.jobinfos[name], nil
}

// Saves the entry for the given job with the given new jobinfo.
func (r *jobsRepo) put(ctx context.Context, j *jinfo) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.jobinfos[j.Name] = j
	return nil
}

// Get the entry for the job with the given name from the repository. If the
// job does not exist return nil,nil
func (r *jobsRepo) GetJobinfo(ctx context.Context, name string) (*hajobs.JobInfo, error) {
	j, err := r.get(ctx, name)
	if err != nil {
		return nil, err
	}
	return jinfoToJobinfo(j)
}

// Saves the entry for the given job with the given new jobinfo.
func (r *jobsRepo) SaveJobinfo(ctx context.Context, job *hajobs.JobInfo) error {
	j := jinfoFromJobinfo(job)
	return r.put(ctx, &j)
}

// Save a new state for the current job, replacing the old state. If the job does
// not exist an error is returned. This updates the last-run timestamp of the job
// so that it can be later on evaluated in the context of the schedule.
// See also Commit().
func (r *jobsRepo) SaveState(ctx context.Context, name string, state []byte) error {
	j, err := r.get(ctx, name)
	if j == nil || err != nil {
		return fmt.Errorf("job %s does not exist or something else went wrong while obtaining it %w", name, err)
	}
	j.State = state
	j.Last = time.Now()
	if err = r.put(ctx, j); err != nil {
		return err
	}
	return nil
}

// Save a new state for the current job, replacing the old state. If the job does
// not exist an error is returned.
// Commit only updates the state, but not the last-run timestamp of the job
// See also Save().
func (r *jobsRepo) Commit(ctx context.Context, name string, state []byte) error {
	j, err := r.get(ctx, name)
	if j == nil || err != nil {
		return fmt.Errorf("job %s does not exist or something else went wrong while obtaining it %w", name, err)
	}
	j.State = state
	if err = r.put(ctx, j); err != nil {
		return err
	}
	return nil
}

// Create or overwrite the job's data in the job repository. Note that this will also
// reset all state and last-run information.
func (r *jobsRepo) CreateJob(ctx context.Context, name string, jobcfg hajobs.JobCfg) error {
	// This is a helper function to deal with unexported fields. This will be improved
	// but currently the package layout I am not sure about.
	job, err := hajobs.ToJobInfo(name, jobcfg)
	if err != nil {
		return err
	}
	if err := r.SaveJobinfo(ctx, &job); err != nil {
		return err
	}
	return nil
}
