# Real-Time Data Pipeline (Kafka + Spark + Airflow)

## Overview
This project implements a real-time data pipeline using Python, PySpark, and Kafka.

## Architecture
Generator → Kafka → Spark → Data Lake

## Features
- Real-time data ingestion
- Stream processing using Spark
- Aggregation of event data
- Layered data storage (raw, processed, aggregated)

## Tech Stack
- Python
- PySpark
- Apache Kafka
- WSL (Linux environment)

## How to Run
1. Start Zookeeper
2. Start Kafka
3. Run producer
4. Run Spark job