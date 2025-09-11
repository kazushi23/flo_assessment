package service

import (
	"flo/assessment/config/log"
	"flo/assessment/entity"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type JobQueueServiceImpl struct{}

// RetrieveQueue returns a job record for a file
func (j *JobQueueServiceImpl) RetrieveQueue(filePath string, db *gorm.DB) (entity.JobQueueEntity, error) {
	var job entity.JobQueueEntity
	err := db.First(&job, "file_path = ?", filePath).Error
	return job, err
}

// InitQueue creates a new job record if not exists
func (j *JobQueueServiceImpl) InitQueue(filePath string, db *gorm.DB) (entity.JobQueueEntity, error) {
	job := entity.JobQueueEntity{
		FilePath: filePath,
		Status:   "pending",
	}
	if err := db.FirstOrCreate(&job, entity.JobQueueEntity{FilePath: filePath}).Error; err != nil {
		log.Logger.Error("Failed to record job in DB", zap.String("file", filePath), zap.Error(err))
		return entity.JobQueueEntity{}, err
	}
	return job, nil
}

// InProgressQueue marks a job as in_progress
func (j *JobQueueServiceImpl) InProgressQueue(job entity.JobQueueEntity, filePath string, db *gorm.DB) error {
	start := time.Now().UTC()
	if err := db.Model(&job).Updates(map[string]interface{}{
		"status":        "in_progress",
		"started_at":    &start,
		"finished_at":   nil,
		"error_message": nil,
	}).Error; err != nil {
		log.Logger.Error("Failed to update job status to in_progress", zap.String("file", filePath), zap.Error(err))
		return err
	}
	return nil
}

// HandleEndQueue marks a job as success or failed
func (j *JobQueueServiceImpl) HandleEndQueue(job entity.JobQueueEntity, err error, filePath string, db *gorm.DB) {
	finish := time.Now().UTC()

	updates := map[string]interface{}{
		"finished_at": &finish,
	}

	if err != nil {
		updates["status"] = "failed"
		updates["error_message"] = err.Error()
		log.Logger.Error("File processing failed", zap.String("file", filePath), zap.Error(err))
	} else {
		updates["status"] = "success"
		log.Logger.Info("File processed successfully", zap.String("file", filePath))
	}

	if dbErr := db.Model(&job).Updates(updates).Error; dbErr != nil {
		log.Logger.Error("Failed to update job completion status", zap.String("file", filePath), zap.Error(dbErr))
	}

}
