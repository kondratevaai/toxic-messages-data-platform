
"""
S3 CSV Producer
===============
Reads text data from an S3 CSV and pushes it to Kafka topic partition 0 in batches.
"""

import os
import sys
import time
import logging
import io
import csv
# from src.logger import setup_logger
from logger import setup_logger

from src.message_producers.producer import ToxicityProducer
from src.utils import load_source_config

try:
    from src.message_producers.s3_csv_producer.utils import Boto3Reader
except ImportError:
    sys.path.insert(0, '/app')
    from src.message_producers.s3_csv_producer.utils import Boto3Reader

# from src.logger import setup_logger

setup_logger()
logger = logging.getLogger(__name__)


def stream_csv_batches(reader, object_path, batch_size, active_col):
    """Stream CSV from S3 via GetObject and yield text batches."""
    
    logger.info(f'Instantiating connection to S3 object: {object_path}')
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
    cfg_path = 'config.json'
    kfk_path = os.getenv('KAFKA_CONFIG')
    src_path = os.getenv('SOURCE_CONFIG')
    s3_producer = ToxicityProducer(kafka_cfg=kfk_path, source_cfg=src_path)

    # business-logic cfg: 
    csv_name = s3_producer.custom_config.get('csv_name')
    active_col = s3_producer.custom_config.get('active_col')

    # base kafka params: 
    batch_size = s3_producer.batch_size
    period_s = s3_producer.fetch_interval_s

    reader = Boto3Reader(cfg_path)
    logger.info(f"S3 reader initialized, bucket: {reader.bucket_name}")

    total_sent = 0
    try:
        while True:
            gen_messages = stream_csv_batches(reader, csv_name, batch_size, active_col)
            for batch_num, batch in enumerate(gen_messages, start=1):
                for text in batch:
                    s3_producer.produce_message(text)
                    total_sent += 1
                s3_producer.producer.flush()
                logger.info(f"Batch {batch_num}: sent {len(batch)} messages (total: {total_sent})")
                time.sleep(period_s)

    except (KeyboardInterrupt, Exception) as e:
        logger.info(f"Stopping S3 CSV producer: {e}")
    finally:
        s3_producer.producer.close()
        del reader
        logger.info(f"Done. Total messages sent: {total_sent}")

main()