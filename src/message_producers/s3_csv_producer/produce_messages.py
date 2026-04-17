
import os
import io
import csv
import sys
import time
import json
import asyncio
import aioboto3
import logging 

from src.message_producers.utils import load_source_config

sys.path.insert(0, '/app')
from src.message_producers.utils import Boto3Reader


async def async_stream_csv_batches(reader, object_path, batch_size=None):
    """
    Async version: Stream CSV from S3 via GetObject and yield text batches without loading full DF.
    Requires aioboto3 and async-compatible reader.
    """

    if batch_size is None:
        batch_size = SOURCE_CONFIG['batch_size']
    active_col = SOURCE_CONFIG['active_col']

    print(f'(async) Instantiating connection to S3 object: {object_path}')

    async with aioboto3.Session().client('s3') as s3_client:
        obj = await s3_client.get_object(Bucket=reader.bucket_name, Key=object_path)
        async with obj['Body'] as stream:
            # Read the entire object as text (could be optimized for very large files)
            content = await stream.read()
            text_stream = io.StringIO(content.decode('utf-8'))
            csv_reader = csv.DictReader(text_stream)

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


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)




SOURCE_CONFIG_PATH = os.getenv('SOURCE_CONFIG', '/app/source_config.json')
SOURCE_CONFIG = load_source_config(SOURCE_CONFIG_PATH)


# def stream_csv_batches(reader, object_path, batch_size=None):
#     """Stream CSV from S3 via GetObject and yield text batches without loading full DF."""
#     if batch_size is None:
#         batch_size = SOURCE_CONFIG['batch_size']
#     active_col = SOURCE_CONFIG['active_col']
#     print(f'Instantiating connection to S3 object: {object_path}')
#     obj = reader.s3_client.get_object(Bucket=reader.bucket_name, Key=object_path)
#     stream = io.TextIOWrapper(obj['Body'], encoding='utf-8')
#     csv_reader = csv.DictReader(stream)

#     batch = []
#     for row in csv_reader:
#         text = row.get(active_col)
#         if text:
#             batch.append(text)
#         if len(batch) >= batch_size:
#             yield batch
#             batch = []
#     if batch:
#         yield batch


def main():
    """Read CSV from S3 in small batches and push text lines to Kafka."""

    config_path = os.getenv('MODEL_CONFIG', '/app/config.json')

    # S3 reader
    reader = Boto3Reader(config_path)
    print(f"S3 reader initialized, bucket: {reader.bucket_name}")

    # Kafka producer
    # producer = create_kafka_producer()

    csv_name = SOURCE_CONFIG['csv_name']
    # kafka_topic = SOURCE_CONFIG['kafka_topic']
    period_s = SOURCE_CONFIG['period_s']

    # total_sent = 0
    # try: 
    #     while True: 
    #         # create new generator for each loop to re-read the CSV from S3, simulating new data arrival: 
    #         gen_messages = stream_csv_batches(reader, csv_name)
    #         for batch_num, batch in enumerate(gen_messages, start=1):
    #             for text in batch:
    #                 # partition 0 is for csv producer, 
    #                 # parition 1 is for online data producer
    #                 producer.send(kafka_topic, value={'text': text}, 
    #                               partition=SOURCE_CONFIG['kafka_partition'])
    #                 total_sent += 1

    #             producer.flush()
    #             # NOTE: now reviewing all the messages using kafka-ui 
    #             # print(f"Batch {batch_num}: sent {len(batch)} messages (total: {total_sent})")
    #             time.sleep(period_s)  # small delay between batches
    # except (KeyboardInterrupt, Exception) as e:
    #     print(f"Stopping producer: {e}")
    # finally: 
    #     producer.close()
    #     del reader
    #     print(f"Done. Total messages sent to '{kafka_topic}': {total_sent}")


# if __name__ == "__main__":
main()