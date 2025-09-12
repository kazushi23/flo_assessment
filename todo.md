1. graceful shutdown for workers
2. redis/rabbitmq for job queues -> offload to another node
3. advanced file validation in preprocessing phase
4. rigourous testing to ensure files of various errors are handled
    - malform values
    - repeated nmi-timestamp with different filecreationdatetime, is it safely reprocessed?
    - failed insert batch, any data lost?
    - supposed valid records insertion vs actual record insertion, any data loss?
    - stress test - monitor hardware levels and find bottlenecks/throttling/memory leak
5. system monitoring with grafana etc.
6. update/check zip handling to ensure it works with current csv handling implementation 
7. startdate will get lost as handlendqueue overwrites it at the moment > need to resolve
8. test run multiple zip files, including handling of error zip files etc
9. validation for zip file, that goes into validation for csv
10. improved/more efficient data reprocessing of failed batch
11. load balancers, maybe spread data processing into multiple nodes from preprocessing server to limit server throttling
12. 

need to do validation? 100/200/300/400/500/900
need to cater for 250/550? or only 100 - 200 - 300 - 900