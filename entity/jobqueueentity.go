package entity

import (
	"time"

	"gorm.io/gorm"
)

type JobQueue struct {
	ID         int64  `gorm:"primaryKey;autoIncrement"`
	FilePath   string `gorm:"column:file_path"`
	Status     string `gorm:"column:status;default:'pending'"` // pending, in_progress, success, failed
	StartedAt  *time.Time
	FinishedAt *time.Time
	ErrorMsg   *string   `gorm:"column:error_message"`
	CreatedAt  time.Time `gorm:"autoCreateTime"`
	UpdatedAt  time.Time `gorm:"autoUpdateTime"`
}

func (JobQueue) TableName() string {
	return "job_queue"
}

func (c *JobQueue) BeforeUpdate(tx *gorm.DB) error {
	tx.Statement.SetColumn("updated_at", time.Now().UnixMilli())
	return nil
}
