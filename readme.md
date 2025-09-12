create a config.toml file mimicking example.config.toml and input username and password where applicable
run "go mod tidy" to download packages needed to run
data ingestion trigger is in cron.go, we can choose which type of nem12 file we want to ingest and change the rate as well
// generate valid nem12 data
// if err := tools.GenerateNEM12Normal(f.name, f.interval, 50, 50); err != nil {
// 	log.Logger.Error("Failed to generate CSV", log.Any("file", f.name), zap.Error(err))
// 	return
// }
// generate malform nem12 data
if err := tools.GenerateNEM12Malformed(f.name, f.interval, 50, 50); err != nil {
    log.Logger.Error("Failed to generate CSV", log.Any("file", f.name), zap.Error(err))
    return
}
run "go run ."


**SETUP**
- src/cron/cron.go

represent 1 node for data ingestion start and pre-processing
runs as a goroutine
generates file every 5 seconds (normal/malform/normal + repeated nmi)
does data validation > if file is malformed, reject and inform data owner
else > enqueue file to worker

- config/worker/workerconfig.go

represent 1 node for job queue and processing valid data in parallel
takes in job into queue based on set up in config.toml
if full, file is stored in redis and there is a retry mechanism to slot job into queue every 1 minute
handle backpressure and has retry mechanism for rejected jobs

- src/service/filecheckerservice.go

validation of files based on the rules i wrote in meterReadingInfo.md
checks 100, 200, 300, 900 basic check
not checking 400, 500, 550 and 250 records for now
returns error for loggin and rejection after processing

- src/service/jobqueueservice.go

utilises redis to store the handling progress of each file + logging
enter queue: pending
start processing: in_progress
fail to insert job: reject
finish processing: failed/success

- src/service/fileprocessservice.go

ProcessFileEntry is the main entry point:
initial phase had zip file handling, which work in iteration 1.0
after much development on csv handling, zip handling has not been updated
this service focus on speed and performance, to write huge amount of lines to db
any insertion goes wrong, fallback to safety mechanism
fail insert data => error log saves and data goes into a staging table
this staging table will be processes at a future date for insertion/update with comparison based on filecreationdate as well (from 100 record)
safe mode will run single threaded

- src/service/csvprocessservice.go

run csv reader + batch data + db writes as a go routine each => all run in parallel to ensure high performance
minimal validation check -> decrease run time for processing csv

- src/service/zipprocessservice.go

this service is to handle zip file, which i realised is a format since it was download from https://www.aemo.com.au/library/procedures#q=nem12&e=0
this iteration is very early, 1.0. csv has gone through multiple fixes, optimisation and test
hence, this service should still be working but yet to test

- src/tools/utilstool.go

generateNEM12() => generator for nem files
@line 39 -> gives semi-unique nem, so no nmi-timestamp conflict issues
@line 40 => gives always the same nem, will always have nmi-timestamp conflict issues
GenerateNEM12Malformed => generate malform csv
utilise these different methods to mimic data ingestion as data coming in maybe malformed, repeated nmi-timestamp or the files has completely no issues as well

- config/circuitbreaker/circuitbreakerconfig.go

circuitbreaker is to mainly handle retries when connection cut off or timeout, safely retry 5 times and the circuit will break, so we stop throttling the db
retries will not occur when its data issue, etc data malform or repeated nmi-timestamp

- src/service/stagingprocessorservice.go

to handle staging table, data that once was rejected
run single thread to attempt cleanups and updates to main table

To see my thought process and thoughts dump: considerations.md
To see what other things i can do to optimise this: todo.md
Assignment writeup: writeup.md