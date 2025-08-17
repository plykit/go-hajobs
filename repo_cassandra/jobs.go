package repo_cassandra

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/plykit/hajobs"
	"time"

	"github.com/gorhill/cronexpr"
)

// Job metadata
type jinfo struct {
	Name        string
	Checksec    int
	Enabled     bool
	Lockttlsec  int
	Schedule    string
	LastUpdated time.Time
	State       []byte
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
		LastUpdated: in.LastUpdated,
		State:       in.State,
	}, nil
}

func jinfoFromJobinfo(in *hajobs.JobInfo) jinfo {
	if in == nil {
		return jinfo{}
	}

	return jinfo{
		Name:        in.Name,
		Checksec:    in.Checksec,
		Enabled:     in.Enabled,
		Lockttlsec:  in.Lockttlsec,
		Schedule:    in.Cronexpr,
		LastUpdated: in.LastUpdated,
		State:       in.State,
	}
}

type JobsRepo struct {
	session *gocql.Session
}

// verify the client implements the jobs repo for the job manager
var _ = hajobs.JobsRepo(&JobsRepo{})

func NewJobsRepo(session *gocql.Session) *JobsRepo {
	return &JobsRepo{
		session: session,
	}
}

// Get the entry for the job with the given name from the repository. If the
// job does not exist return nil,nil
func (r *JobsRepo) get(ctx context.Context, name string) (*jinfo, error) {
	var j jinfo

	query := "select name, checksec, enabled, lockttlsec, schedule, lastupdated, state from job_info WHERE Name = ? LIMIT 1"
	iter := r.session.Query(query, name).Consistency(gocql.One).WithContext(ctx).Iter()

	if !iter.Scan(&j.Name, &j.Checksec, &j.Enabled, &j.Lockttlsec, &j.Schedule, &j.LastUpdated, &j.State) {
		iter.Close()
		return nil, nil
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}
	return &j, nil
}

// Saves the entry for the given job with the given new jobinfo.
func (r *JobsRepo) put(ctx context.Context, j *jinfo) error {
	query := "INSERT INTO job_info (name, checksec, enabled, lockttlsec, schedule, lastupdated, state) VALUES (?, ?, ?, ?, ?, ?, ?)"

	err := r.session.Query(query, j.Name, j.Checksec, j.Enabled, j.Lockttlsec, j.Schedule, j.LastUpdated, j.State).
		WithContext(ctx).
		Exec()

	if err != nil {
		return fmt.Errorf("insert/update job %s: %w", j.Name, err)
	}

	return nil
}

// GetJobinfo gets the entry for the job with the given name from the repository. If the
// job does not exist return nil,nil
func (r *JobsRepo) GetJobinfo(ctx context.Context, name string) (*hajobs.JobInfo, error) {
	j, err := r.get(ctx, name)
	if err != nil {
		return nil, err
	}
	if j == nil {
		return nil, nil
	}
	return jinfoToJobinfo(j)
}

// SaveJobinfo the entry for the given job with the given new jobinfo.
func (r *JobsRepo) SaveJobinfo(ctx context.Context, job *hajobs.JobInfo) error {
	j := jinfoFromJobinfo(job)
	return r.put(ctx, &j)
}

// SaveState a new state for the current job, replacing the old state. If the job does
// not exist an error is returned. This updates the last-run timestamp of the job
// so that it can be later on evaluated in the context of the schedule.
// See also Commit().
func (r *JobsRepo) SaveState(ctx context.Context, name string, state []byte) error {
	j, err := r.get(ctx, name)
	if j == nil || err != nil {
		return fmt.Errorf("job %s does not exist or something else went wrong while obtaining it: %w", name, err)
	}
	j.State = state
	j.LastUpdated = time.Now().UTC()

	if err = r.put(ctx, j); err != nil {
		return err
	}

	return nil
}

// Commit a new state for the current job, replacing the old state. If the job does
// not exist an error is returned.
// Commit only updates the state, but not the last-run timestamp of the job
// See also Save().
func (r *JobsRepo) Commit(ctx context.Context, name string, state []byte) error {
	j, err := r.get(ctx, name)
	if j == nil || err != nil {
		return fmt.Errorf("job %s does not exist or something else went wrong while obtaining it: %w", name, err)
	}

	j.State = state

	if err = r.put(ctx, j); err != nil {
		return fmt.Errorf("failed to commit job %s: %w", name, err)
	}
	return nil
}

// CreateJob or overwrite the job's data in the job repository. Note that this will also
// reset all state and last-run information.
func (r *JobsRepo) CreateJob(ctx context.Context, name string, jobCfg hajobs.JobCfg) error {
	jobInfo, err := hajobs.ToJobInfo(name, jobCfg)
	if err != nil {
		return fmt.Errorf("to JobInfo %w", err)
	}
	if err := r.SaveJobinfo(ctx, &jobInfo); err != nil {
		return fmt.Errorf("create job %s: %w", name, err)
	}
	return nil
}
