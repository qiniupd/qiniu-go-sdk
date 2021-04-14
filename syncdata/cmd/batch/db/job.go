package db

import (
	"database/sql"
	"time"

	"gorm.io/gorm"
)

type Job struct {
	ID         uint64 `gorm:"primaryKey"`
	Cmd        string `gorm:"not null"`
	MicroJobs  []*MicroJob
	Bucket     string `gorm:"not null"`
	BucketDest sql.NullString
	CreatedAt  time.Time `gorm:"not null"`
	FinishedAt sql.NullTime
}

func AutoMigrateTables(db *gorm.DB) error {
	return db.AutoMigrate(&Job{}, &MicroJob{})
}

func (job *Job) BucketDestOrNil() *string {
	if job.BucketDest.Valid {
		return &job.BucketDest.String
	} else {
		return nil
	}
}

func (job *Job) SetBucketDestOrNil(b *string) {
	if b == nil {
		job.BucketDest.Scan(nil)
	} else {
		job.BucketDest.Scan(*b)
	}
}

func (job *Job) FinishedAtOrNil() *time.Time {
	if job.FinishedAt.Valid {
		return &job.FinishedAt.Time
	} else {
		return nil
	}
}

func (job *Job) SetFinishedAtOrNil(t *time.Time) {
	if t == nil {
		job.FinishedAt.Scan(nil)
	} else {
		job.FinishedAt.Scan(*t)
	}
}
