
# from abc import abstractmethod
import json
import logging
import os
import sys
from time import time

# from ..producer import ToxicityProducer, load_source_config
from msg_utils import load_source_config
from kafka import KafkaProducer
# from message_producers.s3_csv_producer.produce_messages import SOURCE_CONFIG


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# SOURCE_CONFIG_PATH = os.getenv('SOURCE_CONFIG', '/app/source_config.json')
# KAFKA_CONFIG_PATH = os.getenv('KAFKA_CONFIG', '/app/kafka_config.json')

class ToxicityProducer:
    def __init__(self, kafka_cfg: str, source_cfg: str):
        
        self.config = self.load_config(kafka_cfg)

        self.custpm_config = self.load_config(source_cfg)

        # basic params of the kafka config: 
        self.kafka_topic = self.config.get('kafka_topic')
        self.kafka_bootstrap = self.config['kafka_bootstrap']
        self.fetch_interval_s = self.config.get('fetch_interval_s')
        self.batch_size = self.config.get('batch_size', 10)
        self.kafka_retries = self.config.get('kafka_retries', 5)
        self.kafka_retry_delay = self.config.get('kafka_retry_delay', 5)

        self.kafka_partition = self.custpm_config.get('kafka_partition')

        # additional params of the source config: 
        # self.source_config = self.load_config(source_cfg_path)

        self.create_kafka_producer()
        
        
    def load_config(self, config_path: str) -> dict:
        return load_source_config(config_path)
    
    def create_kafka_producer(self):

        for attempt in range(1, self.kafka_retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                )
                logger.info(f"Connected to Kafka at {self.kafka_bootstrap}")

                return self.producer
            
            except Exception as e:
                logger.warning(f"Kafka attempt {attempt}/{self.kafka_retries} failed: {e}")
                if attempt < self.kafka_retries:
                    time.sleep(self.kafka_retry_delay)
                    
        # raise ConnectionError(f"Could not connect to Kafka after {self.kafka_retries} attempts")

    def produce_message(self, text: str):
        """Abstract method to produce messages. Must be implemented by subclasses."""
        
        self.producer.send(self.kafka_topic, value={'text': text},
            partition=self.kafka_partition)