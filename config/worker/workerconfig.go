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

// jobQueue holds files to process => this is for rejected queues
var jobQueue chan QueueFileJob

// when queue is full, certain jobs get reject and stored in db
// this function picks up those rejected ones and requeue again
func RequeueRejectedJobs() error {
	if jobQueue == nil {
		return fmt.Errorf("worker pool not started")
	}

	// retrieve rejected jobs from redis
	files, err := service.IJobQueueService.RetrieveRejectedJobs()
	if err != nil {
		log.Logger.Error("Failed to retrieve rejected jobs", zap.Error(err))
		return err
	}

	if len(files) == 0 {
		log.Logger.Info("No rejected jobs to requeue")
		return nil
	}
	// go through all rejected file
	for _, file := range files {
		select {
		// requeue the rejected file
		case jobQueue <- QueueFileJob{Path: file}:
			// remove from reject set
			service.IJobQueueService.RemoveFromReject(file)
			log.Logger.Info("Requeued rejected job", log.Any("file", file))
		default:
			// still full, dont do anything first, wait for next pickup
			log.Logger.Warn("Job queue full, cannot requeue", log.Any("file", file))
		}
	}

	return nil
}

// mechanism to process the rejected queue based on x minutes
// init in main.go > for now, 1 minute
func StartAutoRequeue(intervalMinutes int) {
	if intervalMinutes <= 0 {
		return
	}

	ticker := time.NewTicker(time.Duration(intervalMinutes) * time.Minute)

	go func() {
		for range ticker.C {
			log.Logger.Info("Starting automatic requeue of rejected jobs")
			if err := RequeueRejectedJobs(); err != nil { // push to requeue logi
				log.Logger.Error("Automatic requeue failed", log.Any("error", err))
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

	log.Logger.Info("Worker pool started", log.Any("numWorkers", numWorkers))
}

// worker picks jobs from the queue and processes them
func worker(id, concurrencyPerFile int) {
	log.Logger.Info("Worker started", zap.Int("id", id))
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Error("Worker recovered from panic", log.Any("panic", r))
		}
	}()

	for job := range jobQueue {
		// Retrieve or create job record
		if err := service.IJobQueueService.InProgressQueue(job.Path); err != nil {
			log.Logger.Error("Failed to mark in-progress", log.Any("file", job.Path), zap.Error(err))
			continue
		}

		// utilise singular context throughout entire data processing
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		opts := service.DefaultProcessorOptions() // load processor options
		// init new instance to run throughout entire data processing
		serviceImpl := service.NewFileProcessServiceImpl(ctx, concurrencyPerFile, opts, id, job.Path)
		processErr := serviceImpl.ProcessFileEntry(job.Path) // kickstart data processing

		cancel()

		// mark success/failure
		if err := service.IJobQueueService.HandleEndQueue(job.Path, processErr); err != nil {
			log.Logger.Error("Failed to mark job end", log.Any("file", job.Path), zap.Error(err))
		}
	}
}

// EnqueueFile adds a file to the processing queue
func EnqueueFile(filePath string) {
	select {
	case jobQueue <- QueueFileJob{Path: filePath}:
		// success inserting job into queue
		if err := service.IJobQueueService.InitQueue(filePath); err != nil {
			log.Logger.Error("Failed to initialize job, not enqueuing", log.Any("file", filePath), zap.Error(err))
			return
		}
	default:
		log.Logger.Warn("Job queue full, cannot enqueue file", log.Any("file", filePath))
		// queue full, reject and turn to storing in redis
		if err := service.IJobQueueService.RejectQueue(filePath); err != nil {
			log.Logger.Error("Failed to mark job as reject for retry", log.Any("file", filePath), zap.Error(err))
		} else {
			log.Logger.Warn("Job queue full, persisted for later retry", log.Any("file", filePath))
		}
	}
}
