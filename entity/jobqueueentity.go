package entity

import (
	"time"

	"gorm.io/gorm"
)

type JobQueueEntity struct {
	ID         int64  `gorm:"primaryKey;autoIncrement"`
	FilePath   string `gorm:"column:file_path"`
	Status     string `gorm:"column:status;default:'pending'"` // pending, in_progress, success, failed
	StartedAt  *time.Time
	FinishedAt *time.Time
	ErrorMsg   *string `gorm:"column:error_message"`
	CreatedAt  int64   `json:"created_at" gorm:"autoCreateTime:milli;column:created_at;comment:'Created at'"`
	UpdatedAt  int64   `json:"updated_at" gorm:"autoUpdateTime:milli;column:updated_at;comment:'Updated at'"`
}

func (JobQueueEntity) TableName() string {
	return "job_queue"
}

func (c *JobQueueEntity) BeforeUpdate(tx *gorm.DB) error {
	tx.Statement.SetColumn("updated_at", time.Now().UnixMilli())
	return nil
}
