package cmds

import (
	"context"
	"io"
	"time"

	qiniu_kodo_client_sdk "github.com/qiniupd/qiniu-go-sdk/api.v8/kodocli"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/cmd/batch/db"
	qiniu_sdk "github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
	"gorm.io/gorm"
)

type MicroJob interface {
	Execute(ctx context.Context) io.Reader
}

type Job interface {
	GetMicroJobs(ctx context.Context) ([]MicroJob, error)
}

type Command interface {
	GetJob(ctx context.Context) (Job, error)
	GetConcurrency() uint
	GetLogger() qiniu_kodo_client_sdk.Ilog
}

type BaseJob struct {
	ID         uint64
	Bucket     string
	CreatedAt  time.Time
	FinishedAt *time.Time
	DbRecord   *db.Job
}

func (job *BaseJob) fromDBRecord(dbRecord *db.Job) {
	job.ID = dbRecord.ID
	job.Bucket = dbRecord.Bucket
	job.CreatedAt = dbRecord.CreatedAt
	job.FinishedAt = dbRecord.FinishedAtOrNil()
	job.DbRecord = dbRecord
}

type BaseMicroJob struct {
	ID           uint64
	JobID        uint64
	PartialError *bool
	FinishedAt   *time.Time
	Error        error
	DbRecord     *db.MicroJob
}

func (job *BaseMicroJob) fromDBRecord(dbRecord *db.MicroJob) error {
	job.ID = dbRecord.ID
	job.JobID = dbRecord.JobID
	job.Error = dbRecord.Error()
	job.PartialError = dbRecord.PartialErrorOrNil()
	job.FinishedAt = dbRecord.FinishedAtOrNil()
	job.DbRecord = dbRecord
	return nil
}

type BaseCmd struct {
	Bucket      string
	Lister      *qiniu_sdk.Lister
	Logger      qiniu_kodo_client_sdk.Ilog
	Concurrency uint
	Database    *gorm.DB
}

func (cmd *BaseCmd) GetConcurrency() uint {
	return cmd.Concurrency
}

func (cmd *BaseCmd) GetLogger() qiniu_kodo_client_sdk.Ilog {
	return cmd.Logger
}
