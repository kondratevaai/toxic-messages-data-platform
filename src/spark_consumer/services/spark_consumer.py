"""
Spark Consumer for Toxicity Detection

Reads messages from Kafka topics and sends them to the FastAPI toxicity classifier service.
Processes messages in micro-batches using Spark Structured Streaming.
"""

# import os
# import sys
# import json
import logging
# import requests
# # import threading
# from collections import deque
# from http.server import HTTPServer, BaseHTTPRequestHandler
# from typing import Optional
# from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, get_json_object, window, count, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType
# from src.utils import load_source_config
from utils import load_source_config


from logger import setup_logger


setup_logger()
logger = logging.getLogger(__name__)



KAFKA_MESSAGE_SCHEMA = StructType([
    StructField("text", StringType(), True)
])


class SparkConsumer:
    def __init__(self, config_path: str):
        self.config = load_source_config(config_path)
        # self.classifier = ToxicityClassifierClient(self.config['classifier_url'])

        self.kafka_bootstrap = self.config.get('kafka_bootstrap')
        self.kafka_topics = self.config.get('kafka_topics', [])

        self.topics_partitions = dict()
        # self.get_topics_and_partitions()


    def create_spark_session(self) -> SparkSession:
        """Create and configure Spark session for structured streaming."""

        self.spark = SparkSession \
        .builder \
        .appName("ToxicitySparkConsumer") \
        .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark session created: {self.spark.version}")
        logger.info(f"Connecting to Kafka: {self.kafka_bootstrap}")
        logger.info(f"Subscribing to topics: {self.kafka_topics}")
        
        # return spark


    def create_kafka_stream(self) -> DataFrame:
        """
        Create DataFrame from Kafka source.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Streaming DataFrame from Kafka
        """
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap) \
            .option("subscribe", ",".join(self.kafka_topics)) \
            .option("includeHeaders", "true") \
            .option("failOnDataLoss", self.config['failOnDataLoss']) \
            .load()
        
        # print('readstream created')
        
        # Parse JSON value and extract text field
        df = df.select(
            col("value").cast(StringType()).alias("raw_value"),
            col("timestamp").alias("kafka_timestamp"),
            col("topic"),
            col("partition"),
            col("offset")
        )
        
        # Parse JSON and extract text field
        df = df.select(
            get_json_object(col("raw_value"), "$.text").alias("text"),
            col("kafka_timestamp"),
            col("topic"),
            col("partition"),
            col("offset"),
            current_timestamp().alias("processed_at")
        )
        
        # Filter out null texts
        df = df.filter(col("text").isNotNull())
        
        logger.info("Kafka stream created and configured")
        return df

    def get_topics_and_partitions(self) -> dict:
        """Get all partitions from the topics using existing SparkSession."""

        if self.kafka_topics is None or self.kafka_bootstrap is None:
            logger.warning("Kafka bootstrap servers or topics not configured. \
                           \nCannot fetch topics and partitions.")
            return {}
        
        if isinstance(self.kafka_topics, str):
            self.kafka_topics = [t.strip() for t in self.kafka_topics.split(",")]

        if self.spark is not None: 
            # Use SparkSession to fetch Kafka topics and partitions
            topics_df = self.spark.read.format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap) \
                .option("subscribe", ",".join(self.kafka_topics)) \
                .load()

            topics = topics_df.select("topic", "partition").distinct().collect()

            for row in topics:
                topic = row['topic']
                partition = row['partition']
                if topic not in self.topics_partitions:
                    self.topics_partitions[topic] = []
                self.topics_partitions[topic].append(partition)

            logger.info(f"Fetched topics and partitions: {self.topics_partitions}")

        else: 
            logger.warning("Spark session not initialized. \
                           \nCannot fetch topics and partitions.")
    

        return self.topics_partitions