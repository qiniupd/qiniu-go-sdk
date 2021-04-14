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

const StatCmdName = "stat"

type StatCmd struct {
	BaseCmd
	IsContinue bool
	Job        *StatJob
}

type StatJob struct {
	BaseJob
	Cmd *StatCmd
}

type StatMicroJob struct {
	BaseMicroJob
	Job        *StatJob
	Keys       []string
	FailedKeys []string
}

func (job *StatMicroJob) Execute(ctx context.Context) io.Reader {
	buf := new(bytes.Buffer)
	keys := job.Keys
	if job.Job.Cmd.IsContinue {
		keys = job.continueKeys()
	}
	if len(keys) == 0 {
		return buf
	}
	stats, err := job.Job.Cmd.Lister.StatFilesWithContext(ctx, keys)
	if err != nil {
		buf.WriteString(fmt.Sprintf("Failed to stat files: %s\n", err))
		if err = job.DbRecord.SetResultAsError(ctx, job.Job.Cmd.Database, job.Job.Cmd.Job.DbRecord, err); err != nil {
			buf.WriteString(fmt.Sprintf("Failed to write result to database: %s", err))
		}
		return buf
	}
	var failedKeys []string
	for i, stat := range stats {
		if stat.Code != http.StatusOK {
			buf.WriteString(fmt.Sprintf("%s\tError Code: %d\tMessage: %s\n", job.Keys[i], stat.Code, stat.Error))
			failedKeys = append(failedKeys, job.Keys[i])
		} else {
			buf.WriteString(fmt.Sprintf("%s\t%d\n", job.Keys[i], stat.Data.Fsize))
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

func (job *StatMicroJob) continueKeys() []string {
	if job.FinishedAt == nil {
		return job.Keys
	} else {
		return job.FailedKeys
	}
}

func (cmd *StatCmd) CreateNewJob(ctx context.Context, keys []string, batchSize uint) error {
	return cmd.Database.Transaction(func(tx *gorm.DB) error {
		jobRecord := db.Job{Cmd: StatCmdName, Bucket: cmd.Bucket}
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
		cmd.Job = &StatJob{
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

func (job *StatJob) GetMicroJobs(ctx context.Context) ([]MicroJob, error) {
	var microJobDbRecords []*db.MicroJob
	if err := job.Cmd.Database.WithContext(ctx).Find(&microJobDbRecords, db.MicroJob{JobID: job.ID}).Error; err != nil {
		return nil, err
	}
	microJobs := make([]MicroJob, 0, len(microJobDbRecords))
	for _, dbRecord := range microJobDbRecords {
		microJob := new(StatMicroJob)
		microJob.Job = job
		if err := microJob.fromDBRecord(dbRecord); err != nil {
			return nil, err
		}
		microJobs = append(microJobs, microJob)
	}
	return microJobs, nil
}

func (cmd *StatCmd) GetJob(ctx context.Context) (Job, error) {
	return cmd.Job, nil
}

func (job *StatMicroJob) fromDBRecord(dbRecord *db.MicroJob) error {
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

func (job *StatJob) fromDBRecord(dbRecord *db.Job) {
	job.BaseJob.fromDBRecord(dbRecord)
}

func (job *StatJob) fromDBRecordWithMicroJobs(dbRecord *db.Job) ([]*StatMicroJob, error) {
	job.fromDBRecord(dbRecord)
	microJobs := make([]*StatMicroJob, 0, len(dbRecord.MicroJobs))
	for _, dbRecord := range dbRecord.MicroJobs {
		microJob := new(StatMicroJob)
		microJob.Job = job
		if err := microJob.fromDBRecord(dbRecord); err != nil {
			return nil, err
		}
		microJobs = append(microJobs, microJob)
	}
	return microJobs, nil
}

func (job *StatMicroJob) IsJobFailed(key string) bool {
	for _, fk := range job.FailedKeys {
		if key == fk {
			return true
		}
	}
	return false
}
