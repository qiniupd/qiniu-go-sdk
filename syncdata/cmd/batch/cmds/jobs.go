package cmds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/syncdata/cmd/batch/db"
	"gorm.io/gorm"
)

const JobsCmdName = "jobs"

type JobsCmd struct {
	BaseCmd
	JobId      *uint64
	OnlyFailed bool
}

type showJob struct {
	Cmd *JobsCmd
}

type showMicroJob struct {
	Cmd *JobsCmd
}

func (cmd *JobsCmd) GetJob(ctx context.Context) (Job, error) {
	return &showJob{Cmd: cmd}, nil
}

func (job *showJob) GetMicroJobs(ctx context.Context) ([]MicroJob, error) {
	return []MicroJob{&showMicroJob{Cmd: job.Cmd}}, nil
}

func (job *showMicroJob) Execute(ctx context.Context) io.Reader {
	if job.Cmd.JobId == nil {
		return job.showAllJobs(ctx)
	} else {
		return job.showJobById(ctx, *job.Cmd.JobId)
	}
}

func (job *showMicroJob) showAllJobs(ctx context.Context) io.Reader {
	var (
		jobs []db.Job
		buf  = new(bytes.Buffer)
	)
	if err := job.Cmd.Database.Order("id DESC").Find(&jobs).Error; err != nil {
		buf.WriteString(fmt.Sprintf("Failed to show all jobs: %s\n", err))
	}
	for _, job := range jobs {
		writeJobFromDatabaseIntoBuffer(job, buf)
	}
	return buf
}

func writeJobFromDatabaseIntoBuffer(job db.Job, buf *bytes.Buffer) {
	var (
		bucketDest   = ""
		finishedTime = ""
	)
	if job.BucketDest.Valid {
		bucketDest = fmt.Sprintf("Bucket Dest: %s\t", job.BucketDest.String)
	}
	if job.FinishedAt.Valid {
		finishedTime = job.FinishedAt.Time.Local().Format(time.RFC3339)
	}
	buf.WriteString(fmt.Sprintf("ID: %d\tCmd: %s\tBucket: %s\t%sCreatedTime: %s\tFinishedTime: %s\n", job.ID, job.Cmd, job.Bucket, bucketDest, job.CreatedAt.Local().Format(time.RFC3339), finishedTime))
}

func (job *showMicroJob) showJobById(ctx context.Context, jobId uint64) io.Reader {
	var (
		dbRecord db.Job
		buf      = new(bytes.Buffer)
		err      error
	)
	const (
		StatusDone   = "done"
		StatusFailed = "failed"
	)
	if err = job.Cmd.Database.Preload("MicroJobs").First(&dbRecord, jobId).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			buf.WriteString(fmt.Sprintf("Job %d is not found\n", jobId))
		} else {
			buf.WriteString(fmt.Sprintf("Failed to show job %d: %s\n", jobId, err))
		}
	}
	writeJobFromDatabaseIntoBuffer(dbRecord, buf)
	switch dbRecord.Cmd {
	case StatCmdName:
		var (
			statJob       StatJob
			statMicroJobs []*StatMicroJob
		)
		if statMicroJobs, err = statJob.fromDBRecordWithMicroJobs(&dbRecord); err != nil {
			buf.WriteString(fmt.Sprintf("Failed to parse data from database: %s\n", err))
		}
		for _, statMicroJob := range statMicroJobs {
			if statMicroJob.FinishedAt == nil {
				continue
			}
			for _, key := range statMicroJob.Keys {
				var status = StatusDone
				if statMicroJob.Error != nil {
					status = statMicroJob.Error.Error()
				} else if statMicroJob.IsJobFailed(key) {
					status = StatusFailed
				}
				if job.Cmd.OnlyFailed && status == StatusDone {
					continue
				}
				buf.WriteString(fmt.Sprintf("Key: %s\tStatus: %s\n", key, status))
			}
		}
	case CopyCmdName:
		var (
			copyJob       CopyJob
			copyMicroJobs []*CopyMicroJob
		)
		if copyMicroJobs, err = copyJob.fromDBRecordWithMicroJobs(&dbRecord); err != nil {
			buf.WriteString(fmt.Sprintf("Failed to parse data from database: %s\n", err))
		}
		for _, copyMicroJob := range copyMicroJobs {
			if copyMicroJob.FinishedAt == nil {
				continue
			}
			for _, key := range copyMicroJob.Keys {
				var status = StatusDone
				if copyMicroJob.Error != nil {
					status = copyMicroJob.Error.Error()
				} else if copyMicroJob.IsJobFailed(key) {
					status = StatusFailed
				}
				if job.Cmd.OnlyFailed && status == StatusDone {
					continue
				}
				buf.WriteString(fmt.Sprintf("From: %s\tTo: %s\tStatus: %s\n", key.FromKey, key.ToKey, status))
			}
		}
	case DeleteCmdName:
		var (
			deleteJob       DeleteJob
			deleteMicroJobs []*DeleteMicroJob
		)
		if deleteMicroJobs, err = deleteJob.fromDBRecordWithMicroJobs(&dbRecord); err != nil {
			buf.WriteString(fmt.Sprintf("Failed to parse data from database: %s\n", err))
		}
		for _, deleteMicroJob := range deleteMicroJobs {
			if deleteMicroJob.FinishedAt == nil {
				continue
			}
			for _, key := range deleteMicroJob.Keys {
				var status = StatusDone
				if deleteMicroJob.Error != nil {
					status = deleteMicroJob.Error.Error()
				} else if deleteMicroJob.IsJobFailed(key) {
					status = StatusFailed
				}
				if job.Cmd.OnlyFailed && status == StatusDone {
					continue
				}
				buf.WriteString(fmt.Sprintf("Key: %s\tStatus: %s\n", key, status))
			}
		}
	default:
		buf.WriteString(fmt.Sprintf("Unrecognized command of job: %s\n", dbRecord.Cmd))
	}
	return buf
}
