package entity

import (
	"encoding/json"
	"time"

	"gorm.io/gorm"
)

type ErrorRowsEntity struct {
	ID        int64           `gorm:"primaryKey;autoIncrement"`
	JobID     int64           `gorm:"not null;index;column:job_id"`
	Data      json.RawMessage `gorm:"type:json"`
	Error     string          `gorm:"type:text"`
	FilePath  string          `gorm:"type:text"`
	Resolved  bool            `gorm:"default:false"`
	CreatedAt int64           `gorm:"autoCreateTime:milli;column:created_at"`
	UpdatedAt int64           `gorm:"autoUpdateTime:milli;column:updated_at"`
}

func (ErrorRowsEntity) TableName() string {
	return "error_rows"
}

func (c *ErrorRowsEntity) BeforeUpdate(tx *gorm.DB) error {
	tx.Statement.SetColumn("updated_at", time.Now().UnixMilli())
	return nil
}
