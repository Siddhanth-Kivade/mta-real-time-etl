## Why this project
This project was built to simulate a real-world, production-style data engineering pipeline.
It focuses on idempotent ingestion, layered data modeling (Bronze/Silver/Gold),
and orchestration using Apache Airflow.


# mta-real-time-etl
Production-style ETL pipeline ingesting real-time MTA GTFS data using Airflow, S3, and Snowflake.

## Features
- Idempotent Snowflake ingestion using file-based COPY semantics
- Layered data architecture (Bronze / Silver / Gold)
- Fault-tolerant orchestration with retries and logging
- Designed to run on a schedule without manual intervention

## Design decisions
- Raw data is stored unmodified in Bronze to allow reprocessing
- Silver layer enforces schema and timestamp normalization
- Gold layer provides analytics-ready tables
- Secrets are externalized (no credentials committed)

