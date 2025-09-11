package worker

import (
	"context"
	"flo/assessment/config/log"
	"flo/assessment/src/service"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// QueueFileJob represents a file to process
type QueueFileJob struct {
	Path string
}

// jobQueue holds files to process
var jobQueue chan QueueFileJob

func RequeueRejectedJobs() error {
	if jobQueue == nil {
		return fmt.Errorf("worker pool not started")
	}

	files, err := service.IJobQueueService.RetrieveRejectedJobs()
	if err != nil {
		log.Logger.Error("Failed to retrieve rejected jobs", zap.Error(err))
		return err
	}

	if len(files) == 0 {
		log.Logger.Info("No rejected jobs to requeue")
		return nil
	}

	for _, file := range files {
		select {
		case jobQueue <- QueueFileJob{Path: file}:
			// remove from reject set
			service.IJobQueueService.RemoveFromReject(file)
			log.Logger.Info("Requeued rejected job", zap.String("file", file))
		default:
			log.Logger.Warn("Job queue full, cannot requeue", zap.String("file", file))
		}
	}

	return nil
}

func StartAutoRequeue(intervalMinutes int) {
	if intervalMinutes <= 0 {
		return
	}

	ticker := time.NewTicker(time.Duration(intervalMinutes) * time.Minute)

	go func() {
		for range ticker.C {
			log.Logger.Info("Starting automatic requeue of rejected jobs")
			if err := RequeueRejectedJobs(); err != nil {
				log.Logger.Error("Automatic requeue failed", zap.Error(err))
			}
		}
	}()

}

// StartWorkerPool launches N workers to process files concurrently
func StartWorkerPool(numWorkers, queueSize, concurrencyPerFile int) {
	jobQueue = make(chan QueueFileJob, queueSize)

	for i := 0; i < numWorkers; i++ {
		go worker(i, concurrencyPerFile)
	}

	log.Logger.Info("Worker pool started", zap.Int("numWorkers", numWorkers))
}

// worker picks jobs from the queue and processes them
func worker(id, concurrencyPerFile int) {
	log.Logger.Info("Worker started", zap.Int("id", id))

	for job := range jobQueue {
		// Retrieve or create job record
		if err := service.IJobQueueService.InProgressQueue(job.Path); err != nil {
			log.Logger.Error("Failed to mark in-progress", zap.String("file", job.Path), zap.Error(err))
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		opts := service.DefaultProcessorOptions()
		serviceImpl := service.NewFileProcessServiceImpl(ctx, concurrencyPerFile, opts, id, job.Path)
		processErr := serviceImpl.ProcessFileEntry(job.Path)

		cancel()

		// mark success/failure
		if err := service.IJobQueueService.HandleEndQueue(job.Path, processErr); err != nil {
			log.Logger.Error("Failed to mark job end", zap.String("file", job.Path), zap.Error(err))
		}
	}
}

// EnqueueFile adds a file to the processing queue
func EnqueueFile(filePath string) {
	select {
	case jobQueue <- QueueFileJob{Path: filePath}:
		if err := service.IJobQueueService.InitQueue(filePath); err != nil {
			log.Logger.Error("Failed to initialize job, not enqueuing", zap.String("file", filePath), zap.Error(err))
			return
		}
	default:
		log.Logger.Warn("Job queue full, cannot enqueue file", zap.String("file", filePath))
		if err := service.IJobQueueService.RejectQueue(filePath); err != nil {
			log.Logger.Error("Failed to mark job as reject for retry", zap.String("file", filePath), zap.Error(err))
		} else {
			log.Logger.Warn("Job queue full, persisted for later retry", zap.String("file", filePath))
		}
	}
}
