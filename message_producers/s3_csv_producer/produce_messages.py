import os
import io
import csv
import sys
import time
import json

# add /app to path so mounted utils.py is importable:
sys.path.insert(0, '/app')

from utils import Boto3Reader
from kafka import KafkaProducer


CSV_NAME = 'dump_features_include_numeric_1.csv'
ACTIVE_COL = 'text_save_inform'
KAFKA_TOPIC = 'toxic-messages'
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
BATCH_SIZE = 5
PERIOD_S = 3


def create_kafka_producer(retries=5, delay=5):
    """Create Kafka producer with retry logic for startup ordering."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            )
            print(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except Exception as e:
            print(f"⚠ Kafka connection attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
    raise ConnectionError(f"Could not connect to Kafka after {retries} attempts")


def stream_csv_batches(reader, object_path, batch_size=BATCH_SIZE):
    """Stream CSV from S3 via GetObject and yield text batches without loading full DF."""
    obj = reader.s3_client.get_object(Bucket=reader.bucket_name, Key=object_path)
    stream = io.TextIOWrapper(obj['Body'], encoding='utf-8')
    csv_reader = csv.DictReader(stream)

    batch = []
    for row in csv_reader:
        text = row.get(ACTIVE_COL)
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
    print(f"✓ S3 reader initialized, bucket: {reader.bucket_name}")

    # Kafka producer
    producer = create_kafka_producer()

    total_sent = 0
    for batch_num, batch in enumerate(stream_csv_batches(reader, CSV_NAME), start=1):
        for text in batch:
            producer.send(KAFKA_TOPIC, value={'text': text})
            total_sent += 1

        producer.flush()
        print(f"Batch {batch_num}: sent {len(batch)} messages (total: {total_sent})")
        time.sleep(PERIOD_S)  # small delay between batches

    producer.close()
    del reader
    print(f"✓ Done. Total messages sent to '{KAFKA_TOPIC}': {total_sent}")


# if __name__ == "__main__":
main()