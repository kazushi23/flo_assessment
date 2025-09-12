package service

import (
	"encoding/json"
	"fmt"
	"time"

	"flo/assessment/config/log"
	redisUtil "flo/assessment/config/redis"

	"go.uber.org/zap"
)

type JobQueueServiceImpl struct{}

type JobQueueStatus struct {
	Status       string `json:"status"`
	StartedAt    string `json:"started_at"`
	FinishedAt   string `json:"finished_at"`
	ErrorMessage string `json:"error_message"`
}

// InitQueue adds a new job to Redis and enqueues it
func (j *JobQueueServiceImpl) InitQueue(filePath string) error {
	log.Logger.Info("JOB ENTER QUEUE: ", log.Any("path", filePath))

	rc, err := redisUtil.GetRedisClient()
	if err != nil {
		return err
	}

	key := fmt.Sprintf("job:%s", filePath)

	exists, err := rc.Exists(rc.Context(), key).Result()
	if err != nil {
		return err
	}

	if exists == 0 {
		job := JobQueueStatus{
			Status:       "pending",
			StartedAt:    "",
			FinishedAt:   "",
			ErrorMessage: "",
		}
		data, _ := json.Marshal(job)
		if err := rc.RSet(key, data, 0).Err(); err != nil {
			log.Logger.Error("Failed to set job in Redis", log.Any("file", filePath), zap.Error(err))
			return err
		}
	}

	return nil
}

// RejectQueue marks a job as rejected and adds to the reject set
func (j *JobQueueServiceImpl) RejectQueue(filePath string) error {
	log.Logger.Info("JOB REJECTED: ", log.Any("path", filePath))

	rc, err := redisUtil.GetRedisClient()
	if err != nil {
		return err
	}

	key := fmt.Sprintf("job:%s", filePath)
	job := JobQueueStatus{
		Status:       "reject",
		StartedAt:    "",
		FinishedAt:   "",
		ErrorMessage: "",
	}
	data, _ := json.Marshal(job)
	if err := rc.RSet(key, data, 0).Err(); err != nil {
		log.Logger.Error("Failed to mark job as reject", log.Any("file", filePath), zap.Error(err))
		return err
	}

	// Add to reject set
	if err := rc.SAdd(rc.Context(), "job_status:reject", filePath).Err(); err != nil {
		log.Logger.Error("Failed to add job to reject set", log.Any("file", filePath), zap.Error(err))
		return err
	}

	return nil
}

// InProgressQueue marks a job as in_progress
func (j *JobQueueServiceImpl) InProgressQueue(filePath string) error {
	log.Logger.Info("JOB START EXECUTION: ", log.Any("path", filePath))

	rc, err := redisUtil.GetRedisClient()
	if err != nil {
		return err
	}

	key := fmt.Sprintf("job:%s", filePath)

	job := JobQueueStatus{
		Status:       "in_progress",
		StartedAt:    time.Now().UTC().Format(time.RFC3339),
		FinishedAt:   "",
		ErrorMessage: "",
	}
	data, _ := json.Marshal(job)
	if err := rc.RSet(key, data, 0).Err(); err != nil {
		log.Logger.Error("Failed to mark job in_progress", log.Any("file", filePath), zap.Error(err))
		return err
	}

	return nil
}

// HandleEndQueue marks a job as success or failed
func (j *JobQueueServiceImpl) HandleEndQueue(filePath string, processErr error) error {
	log.Logger.Info("JOB COMPLETED: ", log.Any("err", processErr), log.Any("path", filePath))

	rc, err := redisUtil.GetRedisClient()
	if err != nil {
		return err
	}

	key := fmt.Sprintf("job:%s", filePath)
	var job JobQueueStatus

	if processErr != nil {
		job = JobQueueStatus{
			Status:       "failed",
			FinishedAt:   time.Now().UTC().Format(time.RFC3339),
			ErrorMessage: processErr.Error(),
		}
		data, _ := json.Marshal(job)
		if err := rc.RSet(key, data, 0).Err(); err != nil {
			log.Logger.Error("Failed to mark job rejected", log.Any("file", filePath), log.Any("error", processErr.Error()), zap.Error(err))
			return err
		}

		if err := rc.SAdd(rc.Context(), "job_status:reject", filePath).Err(); err != nil { // add to reject queue
			log.Logger.Error("Failed to add job to reject set", log.Any("file", filePath), zap.Error(err))
		}
	} else {
		job = JobQueueStatus{
			Status:       "success",
			FinishedAt:   time.Now().UTC().Format(time.RFC3339),
			ErrorMessage: "",
		}
		data, _ := json.Marshal(job)
		if err := rc.RSet(key, data, 0).Err(); err != nil {
			log.Logger.Error("Failed to mark job success", log.Any("file", filePath), zap.Error(err))
			return err
		}
	}

	return nil
}

// RetrieveRejectedJobs returns all filePaths in the reject set
func (j *JobQueueServiceImpl) RetrieveRejectedJobs() ([]string, error) {
	rc, err := redisUtil.GetRedisClient()
	if err != nil {
		return nil, err
	}

	files, err := rc.SMembers(rc.Context(), "job_status:reject").Result() // get all filepaths from reject queue
	if err != nil {
		return nil, err
	}

	return files, nil
}

// RemoveFromReject removes a file from the reject set
func (j *JobQueueServiceImpl) RemoveFromReject(filePath string) error {
	rc, err := redisUtil.GetRedisClient()
	if err != nil {
		return err
	}

	if err := rc.SRem(rc.Context(), "job_status:reject", filePath).Err(); err != nil { // remove from reject queue
		log.Logger.Error("Failed to remove job from reject set", log.Any("file", filePath), zap.Error(err))
		return err
	}

	return nil
}
