package service

var (
	IZipProcessService       = &ZipProcessServiceImpl{}
	IFileProcessService      = &FileProcessServiceImpl{}
	ICsvProcessService       = &CsvProcessServiceImpl{}
	IJobQueueService         = &JobQueueServiceImpl{}
	IFileCheckerService      = &FileCheckerServiceImpl{}
	IStagingProcessorService = &StagingProcessorServiceImpl{}
)
