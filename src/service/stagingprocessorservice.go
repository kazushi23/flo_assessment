package service

import (
	"flo/assessment/config/log"
	"flo/assessment/entity"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type StagingProcessorServiceImpl struct{}

// retrieve from staging table and compare with main table
func (s *StagingProcessorServiceImpl) ProcessStagingBatch(db *gorm.DB, batchSize int) error {
	log.Logger.Info("running processing staging batch")
	var stagingRows []entity.StagingMeterReadingEntity

	// pull limited batch for processing
	if err := db.Limit(batchSize).Order("created_at asc").Find(&stagingRows).Error; err != nil {
		return err
	}

	if len(stagingRows) == 0 {
		log.Logger.Info("No staging rows to process")
		return nil
	}

	for _, row := range stagingRows {
		var main entity.MeterReadingsEntity
		err := db.Where("nmi = ? AND timestamp = ?", row.Nmi, row.Timestamp).
			First(&main).Error

		// if previously failed but record dont exist, just insert
		if err == gorm.ErrRecordNotFound {
			// insert if not exists
			if err := db.Create(&entity.MeterReadingsEntity{
				Id:               row.Id,
				Nmi:              row.Nmi,
				Timestamp:        row.Timestamp,
				FileCreationDate: row.FileCreationDate,
				Consumption:      row.Consumption,
			}).Error; err != nil {
				log.Logger.Error("Failed to insert into main table", zap.Error(err))
				continue
			}
		} else if err == nil {
			// compare file creation date, update if newer
			if row.FileCreationDate.After(main.FileCreationDate) {
				main.Consumption = row.Consumption
				main.FileCreationDate = row.FileCreationDate

				if err := db.Save(&main).Error; err != nil {
					log.Logger.Error("Failed to update main table", zap.Error(err))
					continue
				}
			}
		} else {
			log.Logger.Error("Failed to query main table", zap.Error(err))
			continue
		}

		// delete staging row once processed
		if err := db.Delete(&entity.StagingMeterReadingEntity{}, "id = ?", row.Id).Error; err != nil {
			log.Logger.Error("Failed to delete staging row", zap.Error(err))
		}
	}

	return nil
}
