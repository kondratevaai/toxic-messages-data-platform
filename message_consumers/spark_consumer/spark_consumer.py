"""
Spark Consumer for Toxicity Detection
======================================

Reads messages from Kafka topics and sends them to the FastAPI toxicity classifier service.
Processes messages in micro-batches using Spark Structured Streaming.

Configuration:
    KAFKA_BOOTSTRAP: Kafka bootstrap servers (default: 'kafka:9092')
    KAFKA_TOPICS: Comma-separated Kafka topics to consume (default: 'toxic-messages')
    API_URL: FastAPI service URL (default: 'http://backend-api:8000')
    API_TOKEN: Authentication token for FastAPI (optional)
    BATCH_TIMEOUT_MS: Trigger interval in milliseconds (default: 10000)
    CHECKPOINT_DIR: Spark checkpoint directory (default: '/tmp/spark_consumer_checkpoint')
"""

import os
import sys
import json
import logging
import requests
from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, get_json_object, window, count, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/spark_consumer.log")
    ]
)
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
KAFKA_TOPICS = os.getenv('KAFKA_TOPICS', 'toxic-messages').split(',')
API_URL = os.getenv('API_URL', 'http://backend-api:8000')
API_TOKEN = os.getenv('API_TOKEN')
BATCH_TIMEOUT_MS = int(os.getenv('BATCH_TIMEOUT_MS', '10000'))
CHECKPOINT_DIR = os.getenv('CHECKPOINT_DIR', '/tmp/spark_consumer_checkpoint')
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
REQUEST_TIMEOUT_S = int(os.getenv('REQUEST_TIMEOUT_S', '30'))



KAFKA_MESSAGE_SCHEMA = StructType([
    StructField("text", StringType(), True)
])

class ToxicityClassifierClient:
    """Client for communicating with FastAPI toxicity classifier."""

    def __init__(self, base_url: str, token: Optional[str] = None, timeout: int = 30):
        """
        Initialize the classifier client.
        
        Args:
            base_url: Base URL of FastAPI service (e.g., 'http://backend-api:8000')
            token: Optional JWT/Bearer token for authentication
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.timeout = timeout
        self.session = requests.Session()
        
        if token:
            self.session.headers.update({
                'Authorization': f'Bearer {token}' if not token.startswith('Bearer ') 
                                 else token
            })
    
    def classify(self, text: str) -> dict:
        """
        Send text to classifier and get toxicity prediction.
        
        Args:
            text: Raw text to classify
            
        Returns:
            Dictionary with prediction results
            
        Raises:
            requests.RequestException: If API call fails
        """
        url = f"{self.base_url}/forward"
        payload = {"text_raw": text}
        
        try:
            response = self.session.post(
                url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            raise requests.RequestException(
                f"Request timeout after {self.timeout}s for text: {text[:100]}"
            )
        except requests.exceptions.ConnectionError as e:
            raise requests.RequestException(f"Connection error to {url}: {e}")
        except requests.exceptions.HTTPError as e:
            error_detail = response.text if hasattr(e, 'response') else str(e)
            raise requests.RequestException(
                f"HTTP error from {url}: {response.status_code} - {error_detail}"
            )
    
    def close(self):
        """Close the session."""
        self.session.close()


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Process a batch of messages from Kafka and send to classifier.
    
    Args:
        batch_df: DataFrame containing the batch of messages
        batch_id: Batch identifier
    """
    if batch_df.count() == 0:
        logger.debug(f"Batch {batch_id}: Empty batch, skipping")
        return
    
    logger.info(f"Processing batch {batch_id} with {batch_df.count()} messages")
    
    try:
        # Initialize classifier client
        classifier = ToxicityClassifierClient(
            base_url=API_URL,
            token=API_TOKEN,
            timeout=REQUEST_TIMEOUT_S
        )
        
        # Collect texts from the batch (limit to avoid memory issues)
        texts = batch_df.select("text").collect()
        
        successful = 0
        failed = 0
        
        for row in texts:
            text = row["text"]
            
            if not text or len(text.strip()) == 0:
                logger.warning(f"Batch {batch_id}: Skipping empty text")
                failed += 1
                continue
            
            # Retry logic for failed requests
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    result = classifier.classify(text)
                    
                    logger.debug(
                        f"Batch {batch_id}: Text classified | "
                        f"Prediction: {result.get('prediction')} | "
                        f"Time: {result.get('processing_time_ms', 'N/A')}ms"
                    )
                    successful += 1
                    break
                    
                except requests.RequestException as e:
                    logger.warning(
                        f"Batch {batch_id}: API call attempt {attempt}/{MAX_RETRIES} failed: {e}"
                    )
                    
                    if attempt == MAX_RETRIES:
                        logger.error(
                            f"Batch {batch_id}: Failed to classify after {MAX_RETRIES} attempts: "
                            f"{text[:100]}"
                        )
                        failed += 1
                    else:
                        import time
                        time.sleep(2 ** (attempt - 1))  # Exponential backoff
        
        classifier.close()
        
        logger.info(
            f"Batch {batch_id} completed: {successful} successful, {failed} failed"
        )
        
    except Exception as e:
        logger.error(f"Batch {batch_id}: Unexpected error in processing: {e}", exc_info=True)


def create_spark_session() -> SparkSession:
    """
    Create and configure Spark session for structured streaming.
    
    Returns:
        Configured SparkSession
    """
    
        # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        # .config("spark.sql.session.timeZone", "UTC") \
    # spark = SparkSession.builder \
    #     .appName("ToxicitySparkConsumer") \
    #     .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    #     .config("spark.streaming.socketReceiveBuffer.size", "67108864") \
    #     .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
    #     .getOrCreate()
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
    logger.info(f"Connecting to Kafka: {KAFKA_BOOTSTRAP}")
    logger.info(f"Subscribing to topics: {KAFKA_TOPICS}")
    
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
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "toxic-messages") \
        .option("includeHeaders", "true") \
        .load()
    
    print('readstream created')
    
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


def main():
    """Main entry point for the Spark consumer."""
    
    logger.info("Starting Toxicity Spark Consumer")
    logger.info(f"Configuration:")
    logger.info(f"  Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
    logger.info(f"  Kafka Topics: {KAFKA_TOPICS}")
    logger.info(f"  API URL: {API_URL}")
    logger.info(f"  Batch Timeout: {BATCH_TIMEOUT_MS}ms")
    logger.info(f"  Checkpoint Dir: {CHECKPOINT_DIR}")
    logger.info(f"  Max Retries: {MAX_RETRIES}")
    
    spark = None
    try:
        # Ensure checkpoint directory exists
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)
        
        # Create Spark session
        spark = create_spark_session()
        
        # Create Kafka stream
        kafka_df = create_kafka_stream(spark)
        
        # Write stream with batch processing
        query = kafka_df \
            .writeStream \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", CHECKPOINT_DIR) \
            .trigger(processingTime=f"{BATCH_TIMEOUT_MS // 1000} seconds") \
            .start()
        
        logger.info("Streaming query started successfully")
        logger.info("Waiting for streaming data...")
        
        # Keep the application running
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error in consumer: {e}", exc_info=True)
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
