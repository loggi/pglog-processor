# pglog-processor
Process PostgreSQL fetched logs, making them ready to be sent to Graphite (soon) and
Elasticsearch

Given a directory full of pg log files, pglog-processor converts them, marking
each file as done simply appending a suffix to its filename.

To download pg logs from Amazon RDS see [pglog-fetcher](https://github.com/loggi/pglog-fetcher).
