package cmds

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"

	qiniu_kodo_sdk "github.com/qiniupd/qiniu-go-sdk/api.v8/kodo"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/cmd/batch/db"
	"gorm.io/gorm"
)

const CopyCmdName = "copy"

type CopyCmd struct {
	BaseCmd
	IsContinue bool
	Job        *CopyJob
}

type CopyJob struct {
	BaseJob
	Cmd        *CopyCmd
	BucketDest *string
}

type CopyMicroJob struct {
	BaseMicroJob
	Job        *CopyJob
	Keys       []CopyKeys
	FailedKeys []CopyKeys
}

type CopyKeys struct {
	FromKey string
	ToKey   string
}

func (job *CopyMicroJob) Execute(ctx context.Context) io.Reader {
	buf := new(bytes.Buffer)
	var bucketDest = ""
	if job.Job.BucketDest != nil {
		bucketDest = *job.Job.BucketDest
	}
	keys := job.Keys
	if job.Job.Cmd.IsContinue {
		keys = job.continueKeys()
	}
	if len(keys) == 0 {
		return buf
	}
	results, err := job.Job.Cmd.Lister.CopyFilesToWithContext(ctx, bucketDest, fromCopyKeysToKodoKeyPairs(keys))
	if err != nil {
		buf.WriteString(fmt.Sprintf("Failed to copy files: %s\n", err))
		if err = job.DbRecord.SetResultAsError(ctx, job.Job.Cmd.Database, job.Job.Cmd.Job.DbRecord, err); err != nil {
			buf.WriteString(fmt.Sprintf("Failed to write result to database: %s", err))
		}
		return buf
	}
	var failedKeyPairs []db.KeyPair
	for i, result := range results {
		if result.Code != http.StatusOK {
			buf.WriteString(fmt.Sprintf("%s => %s\tError Code: %d\tMessage: %s\n", job.Keys[i].FromKey, job.Keys[i].ToKey, result.Code, result.Error))
			failedKeyPairs = append(failedKeyPairs, db.KeyPair{From: job.Keys[i].FromKey, To: job.Keys[i].ToKey})
		} else {
			buf.WriteString(fmt.Sprintf("%s => %s\n", job.Keys[i].FromKey, job.Keys[i].ToKey))
		}
	}
	if len(failedKeyPairs) > 0 {
		if err = job.DbRecord.SetResultAsPartialErrorKeyPairs(ctx, job.Job.Cmd.Database, job.Job.Cmd.Job.DbRecord, failedKeyPairs); err != nil {
			buf.WriteString(fmt.Sprintf("Failed to write result to database: %s", err))
		}
	} else {
		if err = job.DbRecord.SetResultAsOk(ctx, job.Job.Cmd.Database, job.Job.Cmd.Job.DbRecord); err != nil {
			buf.WriteString(fmt.Sprintf("Failed to write result to database: %s", err))
		}
	}
	return buf
}

func (job *CopyMicroJob) continueKeys() []CopyKeys {
	if job.FinishedAt == nil {
		return job.Keys
	} else {
		return job.FailedKeys
	}
}

func (cmd *CopyCmd) CreateNewJob(ctx context.Context, bucketDest string, copyKeys []CopyKeys, batchSize uint) error {
	return cmd.Database.Transaction(func(tx *gorm.DB) error {
		jobRecord := db.Job{Cmd: CopyCmdName, Bucket: cmd.Bucket}
		if bucketDest != "" {
			if err := jobRecord.BucketDest.Scan(bucketDest); err != nil {
				return err
			}
		}
		for uint(len(copyKeys)) > 0 {
			s := batchSize
			if uint(len(copyKeys)) < s {
				s = uint(len(copyKeys))
			}
			batchKeyPairs := copyKeys[:s]
			copyKeys = copyKeys[s:]
			microJobRecord := new(db.MicroJob)
			if err := microJobRecord.SetKeyPairs(fromCopyKeysToDbKeyPairs(batchKeyPairs)); err != nil {
				return err
			}
			jobRecord.MicroJobs = append(jobRecord.MicroJobs, microJobRecord)
		}
		if err := tx.Create(&jobRecord).Error; err != nil {
			return err
		}
		cmd.Job = &CopyJob{
			BaseJob: BaseJob{
				ID:        jobRecord.ID,
				Bucket:    cmd.Bucket,
				CreatedAt: jobRecord.CreatedAt,
				DbRecord:  &jobRecord,
			},
			Cmd:        cmd,
			BucketDest: &bucketDest,
		}
		return nil
	}, &sql.TxOptions{})
}

func (job *CopyJob) GetMicroJobs(ctx context.Context) ([]MicroJob, error) {
	var microJobDbRecords []*db.MicroJob
	if err := job.Cmd.Database.WithContext(ctx).Find(&microJobDbRecords, db.MicroJob{JobID: job.ID}).Error; err != nil {
		return nil, err
	}
	microJobs := make([]MicroJob, 0, len(microJobDbRecords))
	for _, dbRecord := range microJobDbRecords {
		microJob := new(CopyMicroJob)
		microJob.Job = job
		if err := microJob.fromDBRecord(dbRecord); err != nil {
			return nil, err
		}
		microJobs = append(microJobs, microJob)
	}
	return microJobs, nil
}

func (cmd *CopyCmd) GetJob(ctx context.Context) (Job, error) {
	return cmd.Job, nil
}

func (job *CopyMicroJob) fromDBRecord(dbRecord *db.MicroJob) error {
	if pairs, err := dbRecord.KeyPairs(); err != nil {
		return err
	} else {
		job.Keys = fromDbKeyPairsToCopyKeys(pairs)
	}
	if pairs, err := dbRecord.FailedKeyPairs(); err != nil {
		return err
	} else {
		job.FailedKeys = fromDbKeyPairsToCopyKeys(pairs)
	}
	if err := job.BaseMicroJob.fromDBRecord(dbRecord); err != nil {
		return err
	}
	return nil
}

func (job *CopyJob) fromDBRecord(dbRecord *db.Job) {
	job.BucketDest = dbRecord.BucketDestOrNil()
	job.BaseJob.fromDBRecord(dbRecord)
}

func (job *CopyJob) fromDBRecordWithMicroJobs(dbRecord *db.Job) ([]*CopyMicroJob, error) {
	job.fromDBRecord(dbRecord)
	microJobs := make([]*CopyMicroJob, 0, len(dbRecord.MicroJobs))
	for _, dbRecord := range dbRecord.MicroJobs {
		microJob := new(CopyMicroJob)
		microJob.Job = job
		if err := microJob.fromDBRecord(dbRecord); err != nil {
			return nil, err
		}
		microJobs = append(microJobs, microJob)
	}
	return microJobs, nil
}

func (job *CopyMicroJob) IsJobFailed(pair CopyKeys) bool {
	for _, fk := range job.FailedKeys {
		if pair == fk {
			return true
		}
	}
	return false
}

func fromKodoKeyPairsToCopyKeys(from []qiniu_kodo_sdk.KeyPair) []CopyKeys {
	to := make([]CopyKeys, len(from))
	for i, keyPair := range from {
		to[i] = CopyKeys{FromKey: keyPair.Src, ToKey: keyPair.Dest}
	}
	return to
}

func fromCopyKeysToKodoKeyPairs(from []CopyKeys) []qiniu_kodo_sdk.KeyPair {
	to := make([]qiniu_kodo_sdk.KeyPair, len(from))
	for i, copyKey := range from {
		to[i] = qiniu_kodo_sdk.KeyPair{Src: copyKey.FromKey, Dest: copyKey.ToKey}
	}
	return to
}

func fromCopyKeysToDbKeyPairs(from []CopyKeys) []db.KeyPair {
	to := make([]db.KeyPair, len(from))
	for i, copyKey := range from {
		to[i] = db.KeyPair{From: copyKey.FromKey, To: copyKey.ToKey}
	}
	return to
}

func fromDbKeyPairsToCopyKeys(from []db.KeyPair) []CopyKeys {
	to := make([]CopyKeys, len(from))
	for i, copyKey := range from {
		to[i] = CopyKeys{FromKey: copyKey.From, ToKey: copyKey.To}
	}
	return to
}
