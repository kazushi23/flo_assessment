allow single file upload and zip file uploads
what if zip file uploads are embedded? like what was downloaded in the sample
need to do validation? 100/200/300/400/500/900
note the timing format and intervals
how are files sent to energy providers like flo?
how to upload retrieved file?
file format csv or MDFF?
need to cater for 250/550? or only 100 - 200 - 300 - 900

Q: find out how does these files arrive in energy companies > affects how to process the files > api or batch (cron) processing > validation > write to db
A: Assuming files are provided every hour, i will run an hourly job to process the data

if given more time, i can do checks on ram availability and file sizes to optimise further the processing of files.
maybe also write in the language that flo uses? for now golang, i can write the fastest so i use it

added batch processing to ensure that server is constantly processing data at the background

sudden eureka, create a mock data ingestion pipeline (cron) > generate a file > then server will process the file. run with high data ingestion frequency and filesize

maintain table for job queue? then see which failed

after file has been process and success, delete file
if fail, keep file
maintain logs as well

further implementations can be retry mechanism and raising of issue

job queues can utilise redis or RabbitMQ instead of in-mem queye that was implemented > but not required to do so > maybe will move to redis

can setup monitoring for the queues and retry and server health with grafana

Availability as well, if server stops, what about the current queue and the impending data ingestion?

files that failed, keep, if success, delete
retry mechanism
rate limiting / throttling
backpressure
set concurrency etc to config.toml
persistent queue - consider redis, reabbitmq, kafka
mertrics and observability
validation of data and rules for 100/200/300/400/500/900/250/550
remove file if no error
dont fail entire insert if any rows have issue

Critical Production Issues to Address
1. Data Validation & NEM12 Format Compliance

Missing NEM12 header validation: You're not validating the mandatory NEM12 header record (100 record type)
No checksum validation: NEM12 files include checksums in 900 records that must be validated
Missing mandatory field validation: Need to validate required fields like version number, creation date/time, from participant, to participant
No file structure validation: Should validate the logical sequence of records (100 → 200 → 300 → 900)
Quality flags handling: NEM12 includes quality and method flags for each reading that you're not processing
Missing validation for NMI format: Should validate NMI checksums and format compliance

2. Error Handling & Recovery

No circuit breaker pattern: If database goes down, you'll overwhelm it with retries
Missing dead letter queue: Failed jobs have no retry mechanism or manual intervention path
No graceful shutdown: Workers don't handle shutdown signals properly
Context cancellation not propagated: CSV processing doesn't fully respect context cancellation
Memory leaks potential: Large files could cause OOM without proper streaming limits

3. Monitoring & Observability

No metrics collection: Need Prometheus/similar metrics for job processing rates, error rates, processing times
No health checks: Application needs health endpoints for load balancers
Insufficient structured logging: Missing request IDs, user context, processing stages
No alerting: Need alerts for processing failures, queue buildup, database issues
No distributed tracing: Can't track requests across services

4. Security & Compliance

No file size limits: Could be vulnerable to DoS attacks with massive files
No virus scanning: Files should be scanned before processing
Missing audit trails: Need to track who uploaded what and when
No data encryption: Sensitive meter data should be encrypted at rest
No access controls: Missing authentication/authorization for file uploads

5. Performance & Scalability

No connection pooling limits: Database connections not properly managed
Missing backpressure handling: Can overwhelm downstream systems
No caching strategy: Repeated NMI lookups could benefit from Redis cache
Inefficient batch processing: Fixed batch sizes might not be optimal
No horizontal scaling: Workers are tied to single instance

6. Data Integrity & Consistency

No transaction management: Partial failures could leave inconsistent state
Missing data deduplication: Same file processed twice could create duplicates
No data retention policies: Old data accumulation without cleanup
Missing backup/restore procedures: No disaster recovery plan

7. Configuration & Environment Management

Hardcoded timeouts: Should be configurable per environment
No feature flags: Can't disable features without code changes
Missing environment-specific configs: Dev/staging/prod configurations not separated
No secrets management: Database credentials should use proper secret management

Immediate Actions for Today's Production Deployment
High Priority (Must Fix Before Go-Live):

Add file size validation and limits
Implement proper NEM12 header validation
Add database connection limits and timeouts
Implement graceful shutdown handling
Add basic health check endpoints
Set up proper logging levels and structured logging
Add Redis for job state persistence
Implement basic retry logic with exponential backoff

Medium Priority (Fix Within 24-48 Hours):

Add comprehensive NEM12 validation
Implement circuit breaker pattern
Add metrics collection (Prometheus)
Set up monitoring and alerting
Implement file virus scanning
Add audit logging

Data Preprocessing Recommendations:

Pre-validation service: Validate NEM12 format before queuing
File sanitization: Check for malicious content
Metadata extraction: Extract file info (date ranges, NMI count) before processing
Duplicate detection: Check if file was already processed
Data quality checks: Validate reading values are within expected ranges

Redis Implementation for Job Management:

Use Redis for job state tracking and distributed locking
Implement job heartbeats to detect stuck workers
Store processing progress for large files
Implement job priority queues
Add job expiration and cleanup

Potential issues:

Queue full: If more than 100 files are submitted before workers can dequeue, EnqueueFile will drop files (select default).

Backpressure: You have no blocking or retry mechanism. Files will be lost under heavy load.

Memory usage: Each QueueFileJob holds at least file path metadata; not huge, but for very large file lists, memory grows.

Resolution:

Use a larger queue or blocking enqueue.

Optionally, persist jobs in DB for retry if queue is full.

if err := tools.GenerateNEM12CSV(f.name, f.interval, 50, 50); err != nil {
    log.Logger.Error("Failed to generate CSV", zap.String("file", f.name), zap.Error(err))
    continue
}

currently generates 720000 rows and takes about 1-2 minute. too slow
concurrency = 5
jobqueuesize = 100
numworkers = 10
maxdbconnections = 100
maxdbidleconnections = 100
batchsize = 100

currently generates 720000 rows and takes about 30s. too slow
concurrency = 5
jobqueuesize = 100
numworkers = 10
maxdbconnections = 100
maxdbidleconnections = 100
batchsize = 5000

bad row retry is not good but not targewtting yet since it does hit for now. i am testing on valid file

same timing around 30s
concurrency = 20
jobqueuesize = 200
numworkers = 10
maxdbconnections = 100
maxdbidleconnections = 50
batchsize = 5000

now i need alot of data validation
if nmi-datetime clash, i need to know the previous nmi-datetime-100(filecreationdate) > compare with current 100 filecreationdate > if more than previous > do an update in db record > update the latest nmi-datetime-100(filecreationdate) > i can maintain nmi-datetime-100(filecreationdate) in redis as well

current code will meet with deadlock as processing and write to db run concurrently. parallel write on the same records will deadlock as i generated multiple files of same nmi-datetime....this is an issue

need data preprocessing to figure out if file is valid before we start processing and writing to db.
im thinking if there is a flow issue, i need data validity vs speed 
run parallel -> if data not valid there will be alot issues
run sync -> do checking runtime -> very slow

unless data ingestion pipeline:
ingest data -> validate -> send all valid data to parallel run -> non valid can process and if go through -> run sync
eg. if data repeated, send to another server to run sync update

shifted a check on data ingestion, to validate the file and find existing nmi-timestamp. now it is found, but im not sure what to do with it in terms of industry standard.
// not sure in real energy sector, what to do with it?
// remove those repeated lines from csv and process it, then store repeat in another table as version history?
// reject the file as it is malform? and inform the data provider?

back to testing throttling
if i had more time i will run test scripts, but alot of information gap at the moment, so im testing as though in production environment with mock data ingestion pipeline.

havent test proper validation as i need some mock datasets. my datasets are all generated and idk what are the norms in terms of malform data

if i run all 3, 5 mins interval will cause alot of backpressure in the queue
{5, fmt.Sprintf("nem12_5min_%s.csv", uuidStr)},
{15, fmt.Sprintf("nem12_15min_%s.csv", uuidStr)},
{30, fmt.Sprintf("nem12_30min_%s.csv", uuidStr)},
tools.GenerateNEM12Normal(f.name, f.interval, 50, 50); 

if its just 15 and 30, no issue. further optimisation can be done

write up on why i choose this combination
[process]
concurrency = 5
jobqueuesize = 50
numworkers = 5
maxdbconnections = 100
maxdbidleconnections = 50
batchsize = 2000

should i index nem-timestamp?

when NMIIntervalExists:
i can keep a record in redis nem-timestamp-filecreationdate
remove row from csv
run sync on that one row
    check if previous nem-timestamp-{filecreationdate} older or newer
        if older
            update
send the file to run async

NONO new thinking. check nmi-timestamp [300] + intervaltime [200]
if this 300 record with nmi-timestamp appear before and intervaltime of 200 as well
entire row is going to be duplicate
remove the row from csv and process sync (check the filecreationdatetime)

have not done processrow and in handle 300, i need to create a record in newfileentity. dont want to commit to this yet, as i have a gut feel that this logic to handle 'duplicate' data might be flawed. need more understanding into nem files.

so currently cant run when nmi and timestamp collides, there will be deadlock.

no idea is flawed if a 300 record of time x has 5 min interval, it will still cross datetime with another 300 record of timex 30min interval....