from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

default_args = {
    'owner': 'winderfeld',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

def stream_data():
    import json
    from kafka import KafkaProducer, KafkaAdminClient
    from kafka.admin import NewTopic
    import time
    import logging
    import random
    from fake_log_generator import AWS_SERVICES, GCP_SERVICES, generate_log_data

    topic_name = 'logs'
    bootstrap_servers = ['broker:29092']

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topic_list = admin_client.list_topics()
        if topic_name not in topic_list:
            logging.info(f"Topic '{topic_name}' does not exist. Creating it...")
            topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics([topic])
            logging.info(f"Topic '{topic_name}' created successfully.")
        else:
            logging.info(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logging.error(f"Error checking/creating topic: {e}")
        return

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, max_block_ms=5000)
    curr_time = time.time()
    count = 0

    while True:
        if time.time() > curr_time + 60: 
            print(count)
            break
        try:
            service_type = random.choice(list(AWS_SERVICES.keys()) + list(GCP_SERVICES.keys()))
            instance_name = random.choice(AWS_SERVICES.get(service_type, ['default']) + GCP_SERVICES.get(service_type, ['default']))
            log_data = generate_log_data(service_type, instance_name)
            message = json.dumps(log_data).encode('utf-8')
            logging.info(f"Sending message to topic '{topic_name}': {message}")
            try:
                producer.send(topic_name, message)
            except Exception as e:
                logging.error(f"Error sending message: {e}")
            count+=1
            time.sleep(1)
        except Exception as e:
            logging.exception(f'An error occurred during data streaming:')
            raise

with DAG('log_streaming',
         default_args=default_args,
         schedule_interval='*/10 * * * *', 
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_logs_to_kafka',
        python_callable=stream_data
    )
