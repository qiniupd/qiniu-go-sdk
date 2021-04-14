package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"gorm.io/gorm"
)

type MicroJob struct {
	ID               uint64 `gorm:"primaryKey"`
	JobID            uint64 `gorm:"not null;foreignKey:id"`
	Parameters       string `gorm:"not null"`
	FinishedAt       sql.NullTime
	PartialError     sql.NullBool
	ErrorMessage     sql.NullString
	FailedParameters sql.NullString
}

type Key struct {
	Key string `json:"key"`
}

func (microJob *MicroJob) SetResultAsOk(ctx context.Context, db *gorm.DB, job *Job) error {
	var (
		updatedMicroJob MicroJob
		now             = time.Now()
		result          = false
	)
	updatedMicroJob.SetFinishedAtOrNil(&now)
	updatedMicroJob.SetPartialErrorOrNil(&result)
	return microJob.updateAndCheckJob(ctx, db, job, &updatedMicroJob)
}

func (microJob *MicroJob) SetResultAsPartialErrorKeys(ctx context.Context, db *gorm.DB, job *Job, keys []string) error {
	var (
		updatedMicroJob MicroJob
		now             = time.Now()
		result          = true
	)
	if err := updatedMicroJob.SetFailedKeys(keys); err != nil {
		return err
	}
	updatedMicroJob.SetFinishedAtOrNil(&now)
	updatedMicroJob.SetPartialErrorOrNil(&result)
	return microJob.updateAndCheckJob(ctx, db, job, &updatedMicroJob)
}

func (microJob *MicroJob) SetResultAsPartialErrorKeyPairs(ctx context.Context, db *gorm.DB, job *Job, pairs []KeyPair) error {
	var (
		updatedMicroJob MicroJob
		now             = time.Now()
		result          = true
	)
	if err := updatedMicroJob.SetFailedKeyPairs(pairs); err != nil {
		return err
	}
	updatedMicroJob.SetFinishedAtOrNil(&now)
	updatedMicroJob.SetPartialErrorOrNil(&result)
	return microJob.updateAndCheckJob(ctx, db, job, &updatedMicroJob)
}

func (microJob *MicroJob) SetResultAsError(ctx context.Context, db *gorm.DB, job *Job, apiError error) error {
	var (
		updatedMicroJob MicroJob
		now             = time.Now()
		result          = true
	)
	updatedMicroJob.SetError(apiError)
	updatedMicroJob.SetFinishedAtOrNil(&now)
	updatedMicroJob.SetPartialErrorOrNil(&result)
	return microJob.updateAndCheckJob(ctx, db, job, &updatedMicroJob)
}

var updateLocks sync.Map

func (microJob *MicroJob) updateAndCheckJob(ctx context.Context, db *gorm.DB, job *Job, updated *MicroJob) error {
	var (
		updatedJob Job
		now        = time.Now()
		count      int64
	)
	updatedJob.SetFinishedAtOrNil(&now)

	l, _ := updateLocks.LoadOrStore(microJob.JobID, new(sync.Mutex))
	l.(*sync.Mutex).Lock()
	defer l.(*sync.Mutex).Unlock()

	err := db.Transaction(func(db *gorm.DB) error {
		if err := db.WithContext(ctx).Model(microJob).Select("FinishedAt", "PartialError", "ErrorMessage", "FailedParameters").Updates(&updated).Error; err != nil {
			return err
		}
		if err := db.WithContext(ctx).Model(microJob).Where("job_id = ? AND finished_at IS NULL", microJob.JobID).Count(&count).Error; err != nil {
			return err
		}
		if count == 0 {
			if err := db.WithContext(ctx).Model(&job).Updates(&updatedJob).Error; err != nil {
				return err
			}
		}
		return nil
	}, &sql.TxOptions{})
	return err
}

type KeyPair struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (microJob *MicroJob) Keys() ([]string, error) {
	var keys []Key
	err := json.Unmarshal([]byte(microJob.Parameters), &keys)
	if err != nil {
		return nil, err
	}
	return fromKeysToStringSlice(keys), nil
}

func (microJob *MicroJob) SetKeys(keys []string) error {
	bytes, err := json.Marshal(fromStringSliceToKeys(keys))
	if err != nil {
		return err
	}
	microJob.Parameters = string(bytes)
	return nil
}

func (microJob *MicroJob) KeyPairs() ([]KeyPair, error) {
	var keys []KeyPair
	err := json.Unmarshal([]byte(microJob.Parameters), &keys)
	return keys, err
}

func (microJob *MicroJob) SetKeyPairs(pairs []KeyPair) error {
	bytes, err := json.Marshal(pairs)
	if err != nil {
		return err
	}
	microJob.Parameters = string(bytes)
	return nil
}

func (microJob *MicroJob) Error() error {
	if microJob.ErrorMessage.Valid {
		return errors.New(microJob.ErrorMessage.String)
	} else {
		return nil
	}
}

func (microJob *MicroJob) SetError(err error) {
	if err == nil {
		microJob.ErrorMessage.Scan(nil)
	} else {
		microJob.ErrorMessage.Scan(err.Error())
	}
}

func (microJob *MicroJob) FailedKeys() ([]string, error) {
	var keys []Key
	if !microJob.FailedParameters.Valid {
		return nil, nil
	}
	err := json.Unmarshal([]byte(microJob.FailedParameters.String), &keys)
	if err != nil {
		return nil, err
	}
	return fromKeysToStringSlice(keys), nil
}

func (microJob *MicroJob) SetFailedKeys(keys []string) error {
	bytes, err := json.Marshal(fromStringSliceToKeys(keys))
	if err != nil {
		return err
	}
	microJob.FailedParameters.Scan(string(bytes))
	return nil
}

func (microJob *MicroJob) FailedKeyPairs() ([]KeyPair, error) {
	var keys []KeyPair
	if !microJob.FailedParameters.Valid {
		return nil, nil
	}
	err := json.Unmarshal([]byte(microJob.FailedParameters.String), &keys)
	return keys, err
}

func (microJob *MicroJob) SetFailedKeyPairs(pairs []KeyPair) error {
	bytes, err := json.Marshal(pairs)
	if err != nil {
		return err
	}
	microJob.FailedParameters.Scan(string(bytes))
	return nil
}

func (microJob *MicroJob) PartialErrorOrNil() *bool {
	if microJob.PartialError.Valid {
		return &microJob.PartialError.Bool
	} else {
		return nil
	}
}

func (microJob *MicroJob) SetPartialErrorOrNil(v *bool) {
	if v == nil {
		microJob.PartialError.Scan(nil)
	} else {
		microJob.PartialError.Scan(*v)
	}
}

func (microJob *MicroJob) FinishedAtOrNil() *time.Time {
	if microJob.FinishedAt.Valid {
		return &microJob.FinishedAt.Time
	} else {
		return nil
	}
}

func (microJob *MicroJob) SetFinishedAtOrNil(t *time.Time) {
	if t == nil {
		microJob.FinishedAt.Scan(nil)
	} else {
		microJob.FinishedAt.Scan(*t)
	}
}

func fromStringSliceToKeys(from []string) []Key {
	to := make([]Key, 0, len(from))
	for _, key := range from {
		to = append(to, Key{Key: key})
	}
	return to
}

func fromKeysToStringSlice(from []Key) []string {
	to := make([]string, 0, len(from))
	for _, key := range from {
		to = append(to, key.Key)
	}
	return to
}
