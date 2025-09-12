Write up - In your assessment please also assist us in
answering the following questions:
Q1. What is the rationale for the technologies you have decided to use?
implementation is split into 4 nodes:
1. data ingestion + validation
2. data processing and db insert
3. worker pool and job queue + retry mechanism for rejected jobs from queue full
4. safety mechanism => staging table to hold data that main node(2) failed to insert. run single threaded for data consistency

I have utilise redis for maintaining job queue. read/write speed is faster to maintain status of each job as compared to writing to db. however, this is still running on main node. if main node fails, everything is gone.

circuitbreakers to ensure that db throttling is well managed with retries and trip. failed inserts due to timeout has the chance to re-insert again

worker and goroutine to run processes in parallel, and utilise queue to ensure faster processing and well managed utilisation of hardware. if sudden spike, worker can reject the job and it goes to reject queue which will retry later. without this, the server will crash due to OOM or other factors.

the priority here is speed and performance. hence i have multiple goroutines to handle data processing, batch handling and db writing. however, this is dangrous as the concurrency, worker pool, db connections need extensive testing to ensure no throttling. monitoring and testing needed for production standard.

backpressure handling via reject jobs and retry mechanism. in case the rate of data ingestion is more than the data processing speed.

safety mechanism with staging table to hold data that failed insert. 'main node' is to handle bulk write, up to 61200 records per second. everything runs parallel for the 'main node'. 'safety node' runs single thread to process failed insert data. this is to ensure high throughput for writing to db and 'safety node' ensure reliabilty and consistency. one node running (currently cron) to process failed insert data from main node.

Data preprocessing is a seperate node, mimic with a cron job. it does data validation. any data going into main node for processing, the dataset is valid. reduce runtime checking especially for 300 records, increasing db insertion speed.

the entire pipeline is broken down into 3 parts:
1. data ingestion + validation
2. data processing and db insert
3. worker pool and job queue + retry mechanism for rejected jobs from queue full
4. safety mechanism => staging table to hold data that main node(2) failed to insert. run single threaded for data consistency

Q2. What would you have done differently if you had more time?
I would not have invested so much time into handling zip and nested zip possibilities. this was triggered because i downloaded the dataset from https://www.aemo.com.au/library/procedures#q=nem12&e=0 and it was a zip file. after awhile, i started to shift my focus on the main point: Please ensure the implementation is prepared to handle files of very large sizes. Use any (or no) CSV parser/ORM.

create multiple nodes to ensure speed, reliability and availability:
1. data ingestion + validation
2. data processing and db insert
3. worker pool and job queue + retry mechanism for rejected jobs from queue full
4. safety mechanism => staging table to hold data that main node(2) failed to insert. run single threaded for data consistency

find out more on business requirements as it is closely tied to technical implementation. for instance, if business requirements are realtime data processing or batch processing every hour, the technical implementation will change. if speed and performance is not 100% importance, i can do some validation insert or do not run parallel db insertion, so that i can handle nmi-timestamp well

also, with more time, i need to understand nem12 better, for better file validation and algo to ensure repeated nmi-timestamp, what is the most efficient way to handle (via comparison of 100 recod file creation datetime)

push 1 node (worker) to RabbitMQ or Redis, on a seperate node, rather than within the main node here. All the queuing is offload, worker just pick up jobs. this is for reliability and monitoring.

push 1 node (data preprocessing) for checking and validation then send the queue to rabiitmq or redis. this is to offload the validation and free up processing power for main node (insert db)

push 1 node for retry mechanism (staging table) to run single thread and steadily.

set up monitoring on hardware (cpu/ram) to note throttling and efficiency. have some preventive measures to scale and rely on multiple node to process data if sudden surge occurs.

rigourous testing on dataset as file error has so many possibilities. my validation checks only the base at the moment.

figure a better way for ProcessStagingBatch. what if filecreationdate is the same as in main table? there are alot of things to consider for business use case, eg. how to handle wrong data, what data errors are accepted, what happens when file creation datetime is the same?

find out how does these files arrive in energy companies

graceful shutdown for worker, finish current jobs and block new queues then shutdown.

advanced data preprocessing and file validation for 100/200/300/400/500/900/250/550

Q3. What is the rationale for the design choices that you have made?
there are multiple problems we need to handle.
1. reliability for jobs
2. performance for db insert
3. protection against spikes
4. safety mechanism for failed data writes to ensure data integrity
5. data preprocessing to offload validation in main node
6. split up duties in seperate nodes to ensure scalability, availability and reliability

Infra
1. data ingestion + validation (cron.go and filecheckerservice.go and utilstool.go)
- trigger point => run as a goroutine for each trigger
- generates sample data nem12 csv for processing
- validation of files > reject if issues and log > else enqueue to worker
- data enqueued is assumed to be clean and error proof

2. worker pool and job queue (workerconfig.go and jobqueueservice.go)
- prevent sudden spikes from throttling server
- handles backpressure, if more jobs queue than data is being processed, new jobs are rejected
- rejected job has retry mechanism
- utilise redis to reduce overhead - track pending/in progress/success/failed/rejected jobs
- one instance of fileprocessor per job, from start to end, prevent race condition or memory overwrite due to parallel processing

3. data processing and db insert (fileprocessservice.go and csvprocessservice.go and zipprocessservice.go and circuitbreakerconfig.go)
- run csv reader + batch data + db writes as a go routine each => all run in parallel to ensure high performance
- minimal validation check -> decrease run time for processing csv
- maintains []BatchError for entire job lifetime
- utilise batch insert, batch and errors are helded in singular NewFileProcessServiceImpl instance from start to end for the job
- circuitbreaker for retries if timeout or connection issues. if data issue, there is safety mechanism and circuitbreaker will not trip
- safety mechanism (stagingmeterentity) -> to hold fail db writes and process on the side single threaded

4. safety mechanism (cron.go and main.go and stagingprocessorservice.go)
- take batch size 200 every 10 seconds run single thread and slow
- if data dont exist in db (nmi-timestamp), create new record in meterreading entity
- if data exist, compare file creation datetime. new filecreation datetime will reflect in main table via an update
- data is deleted from staging table

[process]
concurrency = 5
jobqueuesize = 50
numworkers = 5
maxdbconnections = 100
maxdbidleconnections = 50
batchsize = 2000

If each task = ~2000 rows (batch size), then 5 × 2000 = 10,000 rows
2000 rows → 50 × 2000 = 100,000 rows waiting
for now, 1 worker per concurrent task, 5 workers × 2000 rows/batch = 10,000 rows per cycle
5 workers × 10 concurrent queries each = 50 (batch size 2000), maxdbconnection = 100
keep 50 idle for warm connections without consuming too much memory
5 workers → 10,000 rows per batch cycle

Throughput: ~10,000 rows per cycle (with 5 workers).
Queue capacity: 100,000 rows waiting before reject, safe burst handling.
DB load: At most 100 connections, which is reasonable for a medium/high tier MySQL instance.
Resource efficiency: CPU and memory usage kept balanced, avoiding bottlenecks.

Some aspects of the problem statement are intentionally kept open-ended to allow
space for bringing your own thought process & strengths to the table and seeing how
you bind ambiguous aspects of a problem space.

what happened:
1. wrote alot of code on processing nested zip files and run processing with goroutine, but inside the goroutine, it is single threaded.
2. optimise csv parsing with validation
3. utilise batch processing but single threaded writes to db
4. implemented cronjob every 5 seconds to mimic data ingestion flow
5. added worker and retry queue for rejected job, utilise db to maintain the job image. moved to redis for feaster processing and avoid write to db for such things.
6. run db inserts parallel => resulting in deadlock for repeated nmi-timestamp
7. implement retry mechanism for failed insert with processchunk func still in code base but commented out
8. run multiple goroutines in 1 worker for batch data collection, db writes and csv processing
9. move data validation to preprocessing - free up run time for db insert
10. remove retry mechanism, added safety mechanism to store failed batch and reprocessing outside main node