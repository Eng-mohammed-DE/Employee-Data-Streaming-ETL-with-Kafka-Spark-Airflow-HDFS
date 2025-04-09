# 🚀 Real-Time Kafka-Spark-HDFS-Airflow Pipeline

A real-time data pipeline project that simulates employee data, streams it with Kafka, processes it using Apache Spark Structured Streaming, stores it in HDFS (in Parquet format), and orchestrates it all with Apache Airflow.

---

## 🧠 Overview

This project demonstrates how to build a real-time streaming pipeline using:

- **Kafka** for producing and consuming data
- **Spark Structured Streaming** for processing JSON data from Kafka
- **HDFS** to store the processed data in Parquet format
- **Apache Airflow** to orchestrate and automate the pipeline

---

## ⚙️ Tech Stack

| Tool            | Purpose                          |
|-----------------|----------------------------------|
| Python          | Scripting & Airflow DAGs         |
| Apache Kafka    | Real-time data streaming         |
| Apache Spark    | Streaming data processing        |
| Apache Airflow  | Task orchestration               |
| HDFS            | Distributed data storage         |
| Parquet         | Columnar data format             |

---

## 🔄 Pipeline Flow

1. **Kafka Producer** (via Airflow PythonOperator):
   - Simulates employee records (name, department) and sends them to Kafka topic `node4`.

2. **Spark Consumer** (via Airflow SparkSubmitOperator):
   - Reads the Kafka stream, parses JSON data, and writes it to HDFS as Parquet.

3. **Airflow DAG**:
   - Runs the producer → consumer flow with retry policies and monitoring