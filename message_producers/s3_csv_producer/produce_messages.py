import os
import io
import csv
import sys
import time
import json

try: 
    from utils import Boto3Reader
except ImportError as e:
    # working inside liux composed docker seession, need to add /app to path: 
    sys.path.insert(0, '/app')

from kafka import KafkaProducer


def load_source_config(config_path: str) -> dict:
    """Load source configuration from JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)


SOURCE_CONFIG_PATH = os.getenv('SOURCE_CONFIG', '/app/source_config.json')
SOURCE_CONFIG = load_source_config(SOURCE_CONFIG_PATH)


def create_kafka_producer(retries=None, delay=None):
    """Create Kafka producer with retry logic for startup ordering."""
    if retries is None:
        retries = SOURCE_CONFIG.get('kafka_retries', 5)
    if delay is None:
        delay = SOURCE_CONFIG.get('kafka_retry_delay', 5)
    kafka_bootstrap = SOURCE_CONFIG['kafka_bootstrap']
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            )
            print(f"Connected to Kafka at {kafka_bootstrap}")
            return producer
        except Exception as e:
            print(f"Kafka connection attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
    raise ConnectionError(f"Could not connect to Kafka after {retries} attempts")


def stream_csv_batches(reader, object_path, batch_size=None):
    """Stream CSV from S3 via GetObject and yield text batches without loading full DF."""
    if batch_size is None:
        batch_size = SOURCE_CONFIG['batch_size']
    active_col = SOURCE_CONFIG['active_col']
    print(f'Instantiating connection to S3 object: {object_path}')
    obj = reader.s3_client.get_object(Bucket=reader.bucket_name, Key=object_path)
    stream = io.TextIOWrapper(obj['Body'], encoding='utf-8')
    csv_reader = csv.DictReader(stream)

    batch = []
    for row in csv_reader:
        text = row.get(active_col)
        if text:
            batch.append(text)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def main():
    """Read CSV from S3 in small batches and push text lines to Kafka."""

    config_path = os.getenv('MODEL_CONFIG', '/app/config.json')

    # S3 reader
    reader = Boto3Reader(config_path)
    print(f"S3 reader initialized, bucket: {reader.bucket_name}")

    # Kafka producer
    producer = create_kafka_producer()

    csv_name = SOURCE_CONFIG['csv_name']
    kafka_topic = SOURCE_CONFIG['kafka_topic']
    period_s = SOURCE_CONFIG['period_s']

    total_sent = 0
    try: 
        while True: 
            # create new generator for each loop to re-read the CSV from S3, simulating new data arrival: 
            gen_messages = stream_csv_batches(reader, csv_name)
            for batch_num, batch in enumerate(gen_messages, start=1):
                for text in batch:
                    # partition 0 is for csv producer, 
                    # parition 1 is for online data producer
                    producer.send(kafka_topic, value={'text': text}, 
                                  partition=SOURCE_CONFIG['kafka_partition'])
                    total_sent += 1

                producer.flush()
                # NOTE: now reviewing all the messages using kafka-ui 
                # print(f"Batch {batch_num}: sent {len(batch)} messages (total: {total_sent})")
                time.sleep(period_s)  # small delay between batches
    except (KeyboardInterrupt, Exception) as e:
        print(f"Stopping producer: {e}")
    finally: 
        producer.close()
        del reader
        print(f"Done. Total messages sent to '{kafka_topic}': {total_sent}")


# if __name__ == "__main__":
main()