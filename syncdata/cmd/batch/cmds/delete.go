package cmds

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"

	"github.com/qiniupd/qiniu-go-sdk/syncdata/cmd/batch/db"
	"gorm.io/gorm"
)

const DeleteCmdName = "delete"

type DeleteCmd struct {
	BaseCmd
	IsContinue bool
	Job        *DeleteJob
}

type DeleteJob struct {
	BaseJob
	Cmd *DeleteCmd
}

type DeleteMicroJob struct {
	BaseMicroJob
	Job        *DeleteJob
	Keys       []string
	FailedKeys []string
}

func (job *DeleteMicroJob) Execute(ctx context.Context) io.Reader {
	buf := new(bytes.Buffer)
	keys := job.Keys
	if job.Job.Cmd.IsContinue {
		keys = job.continueKeys()
	}
	if len(keys) == 0 {
		return buf
	}
	results, err := job.Job.Cmd.Lister.DeleteFilesWithContext(ctx, keys)
	if err != nil {
		buf.WriteString(fmt.Sprintf("Failed to delete files: %s\n", err))
		if err = job.DbRecord.SetResultAsError(ctx, job.Job.Cmd.Database, job.Job.Cmd.Job.DbRecord, err); err != nil {
			buf.WriteString(fmt.Sprintf("Failed to write result to database: %s", err))
		}
		return buf
	}
	var failedKeys []string
	for i, result := range results {
		if result.Code != http.StatusOK {
			buf.WriteString(fmt.Sprintf("%s\tError Code: %d\tMessage: %s\n", job.Keys[i], result.Code, result.Error))
			failedKeys = append(failedKeys, job.Keys[i])
		} else {
			buf.WriteString(fmt.Sprintf("%s\n", job.Keys[i]))
		}
	}
	if len(failedKeys) > 0 {
		if err = job.DbRecord.SetResultAsPartialErrorKeys(ctx, job.Job.Cmd.Database, job.Job.Cmd.Job.DbRecord, failedKeys); err != nil {
			buf.WriteString(fmt.Sprintf("Failed to write result to database: %s", err))
		}
	} else {
		if err = job.DbRecord.SetResultAsOk(ctx, job.Job.Cmd.Database, job.Job.Cmd.Job.DbRecord); err != nil {
			buf.WriteString(fmt.Sprintf("Failed to write result to database: %s", err))
		}
	}
	return buf
}

func (job *DeleteMicroJob) continueKeys() []string {
	if job.FinishedAt == nil {
		return job.Keys
	} else {
		return job.FailedKeys
	}
}

func (cmd *DeleteCmd) CreateNewJob(ctx context.Context, keys []string, batchSize uint) error {
	return cmd.Database.Transaction(func(tx *gorm.DB) error {
		jobRecord := db.Job{Cmd: DeleteCmdName, Bucket: cmd.Bucket}
		for uint(len(keys)) > 0 {
			s := batchSize
			if uint(len(keys)) < s {
				s = uint(len(keys))
			}
			batchKeys := keys[:s]
			keys = keys[s:]
			microJobRecord := new(db.MicroJob)
			if err := microJobRecord.SetKeys(batchKeys); err != nil {
				return err
			}
			jobRecord.MicroJobs = append(jobRecord.MicroJobs, microJobRecord)
		}
		if err := tx.Create(&jobRecord).Error; err != nil {
			return err
		}
		cmd.Job = &DeleteJob{
			BaseJob: BaseJob{
				ID:        jobRecord.ID,
				Bucket:    cmd.Bucket,
				CreatedAt: jobRecord.CreatedAt,
				DbRecord:  &jobRecord,
			},
			Cmd: cmd,
		}
		return nil
	}, &sql.TxOptions{})
}

func (job *DeleteJob) GetMicroJobs(ctx context.Context) ([]MicroJob, error) {
	var microJobDbRecords []*db.MicroJob
	if err := job.Cmd.Database.WithContext(ctx).Find(&microJobDbRecords, db.MicroJob{JobID: job.ID}).Error; err != nil {
		return nil, err
	}
	microJobs := make([]MicroJob, 0, len(microJobDbRecords))
	for _, dbRecord := range microJobDbRecords {
		microJob := new(DeleteMicroJob)
		microJob.Job = job
		if err := microJob.fromDBRecord(dbRecord); err != nil {
			return nil, err
		}
		microJobs = append(microJobs, microJob)
	}
	return microJobs, nil
}

func (cmd *DeleteCmd) GetJob(ctx context.Context) (Job, error) {
	return cmd.Job, nil
}

func (job *DeleteMicroJob) fromDBRecord(dbRecord *db.MicroJob) error {
	if keys, err := dbRecord.Keys(); err != nil {
		return err
	} else {
		job.Keys = keys
	}
	if failedKeys, err := dbRecord.FailedKeys(); err != nil {
		return err
	} else {
		job.FailedKeys = failedKeys
	}
	if err := job.BaseMicroJob.fromDBRecord(dbRecord); err != nil {
		return err
	}
	return nil
}

func (job *DeleteJob) fromDBRecord(dbRecord *db.Job) {
	job.BaseJob.fromDBRecord(dbRecord)
}

func (job *DeleteJob) fromDBRecordWithMicroJobs(dbRecord *db.Job) ([]*DeleteMicroJob, error) {
	job.fromDBRecord(dbRecord)
	microJobs := make([]*DeleteMicroJob, 0, len(dbRecord.MicroJobs))
	for _, dbRecord := range dbRecord.MicroJobs {
		microJob := new(DeleteMicroJob)
		microJob.Job = job
		if err := microJob.fromDBRecord(dbRecord); err != nil {
			return nil, err
		}
		microJobs = append(microJobs, microJob)
	}
	return microJobs, nil
}

func (job *DeleteMicroJob) IsJobFailed(key string) bool {
	for _, fk := range job.FailedKeys {
		if key == fk {
			return true
		}
	}
	return false
}
