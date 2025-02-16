from kafka import KafkaConsumer
from kafka.errors import KafkaError
from time import time, sleep
from slack_utils import SlackNotifier
from mistral_utils import MistralAnalyzer
from message_formatter import format_slack_blocks
import json
import sys
import traceback
from datetime import datetime

class BatchConsumer:
    def __init__(self, batch_size=10, max_wait_seconds=30, max_retries=5, retry_delay=5):
        self.batch_size = batch_size
        self.max_wait_seconds = max_wait_seconds
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connect_kafka()
        self.slack = SlackNotifier()
        self.mistral = MistralAnalyzer()

    def connect_kafka(self):
        retries = 0
        while retries < self.max_retries:
            try:
                self.consumer = KafkaConsumer(
                    'logs',
                    bootstrap_servers=['broker:29092'],
                    group_id='batch_consumer_group',
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    value_deserializer=lambda x: x
                )
                print(f"{self.get_timestamp()} Successfully connected to Kafka")
                return
            except KafkaError as e:
                retries += 1
                print(f"{self.get_timestamp()} Failed to connect to Kafka (attempt {retries}/{self.max_retries}): {str(e)}")
                if retries < self.max_retries:
                    sleep(self.retry_delay)
                else:
                    print(f"{self.get_timestamp()} Max retries reached, exiting...")
                    sys.exit(1)

    def get_timestamp(self):
        return datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')

    def process_message(self, message):
        try:
            log_data = json.loads(message.value.decode('utf-8'))
            
            if log_data.get('severity') == 'ERROR':
                print(f"{self.get_timestamp()} Processing error message: {log_data['id']}")
                ai_analysis = self.mistral.analyze_error(str(log_data))
                blocks = format_slack_blocks(log_data, ai_analysis)
                self.slack.send_notification(blocks)
                sleep(1)
                
        except Exception as e:
            print(f"{self.get_timestamp()} Error processing message: {str(e)}")
            traceback.print_exc()

    def process_batch(self, batch):
        for message in batch:
            self.process_message(message)
        return len(batch)

    def run(self):
        print(f"{self.get_timestamp()} Starting batch consumer...")
        while True:
            try:
                batch = []
                last_message_time = time()
                
                while len(batch) < self.batch_size:
                    try:
                        message = next(self.consumer, None)
                        current_time = time()
                        
                        if message:
                            batch.append(message)
                            last_message_time = current_time
                        elif batch:  # Process remaining messages
                            break
                        elif current_time - last_message_time > self.max_wait_seconds:
                            # No messages for a while, check connection
                            self.check_connection()
                            last_message_time = current_time
                            
                        if len(batch) >= self.batch_size or (batch and current_time - last_message_time > self.max_wait_seconds):
                            break
                            
                    except Exception as e:
                        print(f"{self.get_timestamp()} Error polling messages: {str(e)}")
                        self.check_connection()
                
                if batch:
                    processed = self.process_batch(batch)
                    print(f"{self.get_timestamp()} Processed batch of {processed} messages")
                    self.consumer.commit()
                    
            except Exception as e:
                print(f"{self.get_timestamp()} Unexpected error: {str(e)}")
                traceback.print_exc()
                sleep(self.retry_delay)

    def check_connection(self):
        try:
            self.consumer.topics()
        except Exception:
            print(f"{self.get_timestamp()} Lost connection to Kafka, attempting to reconnect...")
            self.connect_kafka()

def main():
    while True:
        try:
            consumer = BatchConsumer()
            consumer.run()
        except Exception as e:
            print(f"[{datetime.now()}] Fatal error: {str(e)}")
            traceback.print_exc()
            print("Restarting consumer in 5 seconds...")
            sleep(5)

if __name__ == "__main__":
    main()

