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