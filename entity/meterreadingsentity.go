package entity

import (
	"time"

	"gorm.io/gorm"
)

type MeterReadingsEntity struct {
	Id          string    `json:"id" gorm:"column:id;type:char(36);primaryKey;comment:'id'"` // UUID stored as CHAR(36)
	Nmi         string    `json:"nmi" gorm:"column:nmi;type:varchar(10);not null;index:idx_nmi_timestamp,unique;comment:'NMI'"`
	Timestamp   time.Time `json:"timestamp" gorm:"column:timestamp;type:timestamp;not null;index:idx_nmi_timestamp,unique;comment:'Timestamp'"`
	Consumption float64   `json:"consumption" gorm:"column:consumption;type:decimal(18,6);not null;comment:'Consumption'"` // DECIMAL instead of NUMERIC
	CreatedAt   int64     `json:"created_at" gorm:"autoCreateTime:milli;column:created_at;comment:'Created at'"`
	UpdatedAt   int64     `json:"updated_at" gorm:"autoUpdateTime:milli;column:updated_at;comment:'Updated at'"`
}

func (MeterReadingsEntity) TableName() string {
	return "meter_readings"
}

func (c *MeterReadingsEntity) BeforeUpdate(tx *gorm.DB) error {
	tx.Statement.SetColumn("updated_at", time.Now().UnixMilli())
	return nil
}
