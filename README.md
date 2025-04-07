# Kafka-Cassandra ETL Pipeline with Apache Airflow

## Project Overview

This project demonstrates an ETL (Extract, Transform, Load) pipeline using Apache Kafka, Apache Cassandra, and Apache Airflow. The pipeline extracts random user data from a public API, transforms it, and loads it into a Cassandra database for storage. The pipeline is automated using Airflow DAGs.

## Architecture Overview

The pipeline consists of two main parts:

1. **Producer (Kafka)**: 
   - Fetches random user data from a public API (`https://randomuser.me/api/`).
   - Sends the data to a Kafka topic (`nodes`).
   
2. **Consumer (Kafka + Cassandra)**: 
   - Consumes data from the Kafka topic (`nodes`).
   - Inserts the consumed data into a Cassandra table (`users`).

The entire process is orchestrated using Apache Airflow, which runs the producer and consumer tasks in sequence.

### Technologies Used:
- **Apache Kafka**: For message streaming between the producer and consumer.
- **Apache Cassandra**: For storing the processed data in a NoSQL database.
- **Apache Airflow**: For orchestrating the ETL process.
- **Python**: The ETL logic is implemented using Python.
- **Requests**: For fetching data from the public API.
- **Kafka Python Client**: For producing and consuming Kafka messages.
- **Cassandra Python Driver**: For connecting and inserting data into Cassandra.

## Project Structure

```plaintext
kafka_cassandra_etl/
│
├── dags/
│   └── kafka_cassandra_etl_dag.py   # Airflow DAG file containing the producer and consumer functions.
│
├── requirements.txt                  # Python dependencies
├── README.md                         # Project documentation
└── Dockerfile                        # Docker configuration (optional for containerization)
