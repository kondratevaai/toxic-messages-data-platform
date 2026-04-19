"""
Spark Consumer for Toxicity Detection

Reads messages from Kafka topics and sends them to the FastAPI toxicity classifier service.
Processes messages in micro-batches using Spark Structured Streaming.
"""

import os
import sys
import json
import logging
import requests
import threading
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, get_json_object, window, count, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType
from src.utils import load_source_config


# Partition ID -> human-readable source name mapping
PARTITION_SOURCE_MAP = CONFIG.get('partition_source_map', {
    "0": "s3-csv",
    "1": "reddit"
})

# Thread-safe store for recent predictions (Grafana polls this)
MAX_RECENT = 200
recent_predictions = deque(maxlen=MAX_RECENT)
predictions_lock = threading.Lock()




KAFKA_MESSAGE_SCHEMA = StructType([
    StructField("text", StringType(), True)
])



def process_batch(batch_df: DataFrame, batch_id: int, classifier: ToxicityClassifierClient) -> None:
    """
    Process a batch of messages from Kafka and send to classifier.
    
    Args:
        batch_df: DataFrame containing the batch of messages
        batch_id: Batch identifier
    """
    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id}: Empty batch, skipping")
        return
    
    logger.info(f"Processing batch {batch_id} with {batch_df.count()} messages")
    
    try:
        
        # Collect texts with partition info from the batch
        rows = batch_df.select("text", "partition").collect()
        
        successful = 0
        failed = 0
        
        for row in rows:
            text = row["text"]
            partition = row["partition"]
            
            if not text or len(text.strip()) == 0:
                logger.warning(f"Batch {batch_id}: Skipping empty text")
                failed += 1
                continue
            
            # Retry logic for failed requests
            for attempt in range(1, CONFIG['max_retries'] + 1):
                try:
                    result = classifier.classify(text)
                    
                    source = PARTITION_SOURCE_MAP.get(str(partition), f"partition-{partition}")
                    prediction_entry = {
                        "text": text[:200],
                        "partition": partition,
                        "source": source,
                        "prediction": result.get('prediction'),
                        "prediction_label": "toxic" if result.get('prediction') == 1 else "non_toxic",
                        "processing_time_ms": result.get('processing_time_ms'),
                        "timestamp": datetime.utcnow().isoformat(),
                        "batch_id": batch_id
                    }
                    with predictions_lock:
                        recent_predictions.appendleft(prediction_entry)

                    logger.info(
                        f"Text | {text[:100]} | "
                        f"Batch {batch_id}: Text classified | "
                        f"Source: {source} (partition {partition}) | "
                        f"Prediction: {result.get('prediction')} | "
                        f"Time: {result.get('processing_time_ms', 'N/A')}ms"
                    )
                    successful += 1
                    break
                    
                except requests.RequestException as e:
                    mr = CONFIG['max_retries']
                    logger.warning(
                        f"Batch {batch_id}: API call attempt {attempt}/{mr} failed: {e}"
                    )
                    
                    if attempt == mr:
                        logger.error(
                            f"Batch {batch_id}: Failed to classify after {mr} attempts: "
                            f"{text[:100]}"
                        )
                        failed += 1
                    else:
                        import time
                        time.sleep(2 ** (attempt - 1))  # Exponential backoff

                except Exception as e:
                    logger.error(
                        f"Batch {batch_id}: Unexpected error during classification: {e}"
                    )
                    failed += 1
                    break
        
        classifier.close()
        
        logger.info(
            f"Batch {batch_id} completed: {successful} successful, {failed} failed"
        )
        
    except Exception as e:
        logger.error(f"Batch {batch_id}: Unexpected error in processing: {e}", exc_info=True)


def create_spark_session() -> SparkSession:
    """Create and configure Spark session for structured streaming.
    """
    spark = SparkSession \
    .builder \
    .appName("ToxicitySparkConsumer") \
    .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session created: {spark.version}")
    
    return spark


def create_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Create DataFrame from Kafka source.
    
    Args:
        spark: Active SparkSession
        
    Returns:
        Streaming DataFrame from Kafka
    """
    logger.info(f"Connecting to Kafka: {CONFIG['kafka_bootstrap']}")
    logger.info(f"Subscribing to topics: {CONFIG['kafka_topics']}")
    
    # df = spark.readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    #     .option("subscribe", ",".join(KAFKA_TOPICS)) \
    #     .option("startingOffsets", "latest") \
    #     .option("kafka.session.timeout.ms", "30000") \
    #     .option("maxOffsetsPerTrigger", "1000") \
    #     .load()
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG['kafka_bootstrap']) \
        .option("subscribe", ",".join(CONFIG['kafka_topics'])) \
        .option("includeHeaders", "true") \
        .option("failOnDataLoss", CONFIG['failOnDataLoss']) \
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
