from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from kafka import KafkaProducer, KafkaConsumer
import json
import time
import uuid
from cassandra.cluster import Cluster

default_args = {
    'owner': 'eng-mohammed',
    'start_date': datetime(2025, 4, 5),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    dag_id='kafka_cassandra_ETL',
    default_args=default_args,
    schedule_interval=None,  # No automatic scheduling
    catchup=False
)

# ---------------- PRODUCER FUNCTION ---------------- #
def produce_user():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for _ in range(50):  
        response = requests.get('https://randomuser.me/api/')
        user_data = response.json()['results'][0]
        payload = {
            "name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "email": user_data['email'],
            "phone": user_data['phone']
        }
        producer.send('nodes', payload)
        print(f"✅ Sent to Kafka: {payload}")
        time.sleep(1)  

# ---------------- CONSUMER FUNCTION ---------------- #

def consume_and_store():
    # Connect to Cassandra
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()

    # Create keyspace and table

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS node_keyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace('node_keyspace')

    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY,
            name TEXT,
            email TEXT,
            phone TEXT
        )
    """)

    # Kafka consumer
    consumer = KafkaConsumer(
        'nodes',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=None
    )

    print("🚀 Listening to Kafka and inserting into Cassandra forever...")

    for message in consumer:
        user = message.value
        print(f" Received: {user}")
        session.execute("""
            INSERT INTO users (id, name, email, phone) VALUES (%s, %s, %s, %s)
        """, (uuid.uuid4(), user['name'], user['email'], user['phone']))

# ---------------- TASKS ---------------- #
produce_task = PythonOperator(
    task_id='produce_user_data',
    python_callable=produce_user,
    dag=dag
)

consume_task = PythonOperator(
    task_id='consume_user_data',
    python_callable=consume_and_store,
    dag=dag
)

produce_task >> consume_task
