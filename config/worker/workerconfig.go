package worker

import (
	"context"
	"flo/assessment/config/log"
	"flo/assessment/config/mysql"
	"flo/assessment/src/service"
	"time"

	"go.uber.org/zap"
)

// QueueFileJob represents a file to process
type QueueFileJob struct {
	Path string
}

// jobQueue holds files to process
var jobQueue chan QueueFileJob

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
	db := mysql.GetDB()

	for job := range jobQueue {
		log.Logger.Info("Picked job from queue", zap.Int("worker", id), zap.String("file", job.Path))

		// Retrieve or create job record
		jobRecord, err := service.IJobQueueService.RetrieveQueue(job.Path, db)
		if err != nil {
			jobRecord, err = service.IJobQueueService.InitQueue(job.Path, db)
			if err != nil {
				log.Logger.Error("Failed to create job record, skipping job", zap.String("file", job.Path), zap.Error(err))
				continue
			}
		}

		// Mark job in progress
		if err := service.IJobQueueService.InProgressQueue(jobRecord, job.Path, db); err != nil {
			log.Logger.Error("Failed to update job to in_progress, skipping job", zap.String("file", job.Path), zap.Error(err))
			continue
		}

		// Process the file with context and concurrency
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		opts := service.DefaultProcessorOptions()
		opts.BatchSize = 1000

		serviceImpl := service.NewFileProcessServiceImpl(ctx, concurrencyPerFile, opts)
		processErr := serviceImpl.ProcessFileEntry(job.Path)
		cancel()

		// Update job record on completion
		service.IJobQueueService.HandleEndQueue(jobRecord, processErr, job.Path, db)
	}
}

// EnqueueFile adds a file to the processing queue
func EnqueueFile(filePath string) {
	db := mysql.GetDB()
	if _, err := service.IJobQueueService.InitQueue(filePath, db); err != nil {
		log.Logger.Error("Failed to initialize job, not enqueuing", zap.String("file", filePath), zap.Error(err))
		return
	}

	select {
	case jobQueue <- QueueFileJob{Path: filePath}:
		log.Logger.Info("File enqueued", zap.String("file", filePath))
	default:
		log.Logger.Warn("Job queue full, cannot enqueue file", zap.String("file", filePath))
	}
}
