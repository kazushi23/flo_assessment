1. graceful shutdown
2. restart job mechanism
3. retry mechanism - done
4. redis/rabbitmq for job queues
5. file validation before processing - basic checks
6. redis caching for processing of line items + persistence
7. backpressure (more jobs queued than speed of data processing) - done
8. job queuing isolated from server so the queue can continue while server has downtime etc.
9. 
