package entity

import (
	"time"

	"gorm.io/gorm"
)

type NemFileEntity struct {
	ID               uint      `gorm:"primaryKey;autoIncrement"`
	NMI              string    `gorm:"type:varchar(20);not null"`
	IntervalLength   int       `gorm:"not null"`
	IntervalDate     time.Time `gorm:"type:date;not null"`
	FileCreationDate time.Time `gorm:"not null"`
	CreatedAt        int64     `json:"created_at" gorm:"autoCreateTime:milli;column:created_at;comment:'Created at'"`
	UpdatedAt        int64     `json:"updated_at" gorm:"autoUpdateTime:milli;column:updated_at;comment:'Updated at'"`
}

func (NemFileEntity) TableName() string {
	return "nem_file"
}

func (c *NemFileEntity) BeforeUpdate(tx *gorm.DB) error {
	tx.Statement.SetColumn("updated_at", time.Now().UnixMilli())
	return nil
}
