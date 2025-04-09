from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import random
import time
from kafka import KafkaProducer
import json

default_args = {
    'owner': 'Eng-mohammed',
    'start_date': datetime(2025, 4, 8),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='employee_dag2',
    default_args=default_args,
    schedule_interval=None,  # Set a schedule if needed
    catchup=False
)

def kafka_produce_data():
    """Produces employee data to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    names = ['Ali', 'Omer', 'Hassan', 'Sara', 'Mohammed']
    departments = ['HR', 'Finance', 'Engineering', 'Marketing', 'Sales']

    for _ in range(100):
        employee_data = {
            'name': random.choice(names),
            'department': random.choice(departments)
        }
        print(f"Producing: {employee_data}")
        producer.send('node5', value=employee_data)
        time.sleep(1)

produce_task = PythonOperator(
    task_id='produce_to_kafka',
    python_callable=kafka_produce_data,
    dag=dag
)

# Use SparkSubmitOperator for running Spark job
consume_task = SparkSubmitOperator(
    task_id='consume_with_spark',
    application='/home/eng-mohammed/airflow_orchestration/venv3.10/include/spark_consumer.py', 
    conn_id='spark_default',  
    application_args=[
        "--master", "local[*]",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.commons:commons-pool2:2.11.1,org.slf4j:slf4j-api:1.7.36"
    ],
    dag=dag
)

produce_task >> consume_task