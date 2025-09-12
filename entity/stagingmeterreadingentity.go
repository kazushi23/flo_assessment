package entity

import (
	"time"

	"gorm.io/gorm"
)

type StagingMeterReadingEntity struct {
	Id               string    `json:"id" gorm:"column:id;type:char(36);primaryKey;comment:'id'"` // UUID stored as CHAR(36)
	Nmi              string    `json:"nmi" gorm:"column:nmi;type:varchar(10);not null;comment:'NMI'"`
	Timestamp        time.Time `json:"timestamp" gorm:"column:timestamp;type:timestamp;not null;comment:'Timestamp'"`
	FileCreationDate time.Time `json:"file_creation_date" gorm:"column:file_creation_date;type:timestamp;not null"`
	Consumption      float64   `json:"consumption" gorm:"column:consumption;type:decimal(18,6);not null;comment:'Consumption'"`
	CreatedAt        int64     `json:"created_at" gorm:"autoCreateTime:milli;column:created_at;comment:'Created at'"`
	UpdatedAt        int64     `json:"updated_at" gorm:"autoUpdateTime:milli;column:updated_at;comment:'Updated at'"`
}

func (StagingMeterReadingEntity) TableName() string {
	return "staging_meter_readings"
}

func (c *StagingMeterReadingEntity) BeforeUpdate(tx *gorm.DB) error {
	tx.Statement.SetColumn("updated_at", time.Now().UnixMilli())
	return nil
}
