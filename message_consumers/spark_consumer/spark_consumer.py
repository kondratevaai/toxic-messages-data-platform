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


def load_config(config_path: str) -> dict:
    """Load configuration from JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)


CONFIG_PATH = os.getenv('CONSUMER_CONFIG', '/app/consumer_config.json')
CONFIG = load_config(CONFIG_PATH)



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

    def authenticate(self) -> None:
        """
        Register a service user on the API and set the Bearer token.
        Skips if a token is already configured.
        """
        if self.token:
            return

        url = f"{self.base_url}/register"
        payload = {
            "name": "spark-consumer",
            "email": "spark-consumer@service.local",
        }
        try:
            resp = self.session.post(url, json=payload, timeout=self.timeout)
            if resp.status_code == 400 and "already exists" in resp.text:
                logger.info("Service user already registered, requesting new token")
                
                logger.warning(
                    "Cannot auto-authenticate: service user already exists. "
                    "Set 'api_token' in consumer_config.json."
                )
                return
            resp.raise_for_status()
            data = resp.json()
            self.token = data["access_token"]
            self.session.headers.update({
                'Authorization': f'Bearer {self.token}'
            })
            logger.info("Authenticated with API successfully")
        except Exception as e:
            logger.warning(f"Auto-authentication failed: {e}")
    
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
            for attempt in range(1, CONFIG['max_retries'] + 1):
                try:
                    result = classifier.classify(text)
                    
                    logger.info(
                        f"Text | {text[:100]} | "
                        f"Batch {batch_id}: Text classified | "
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


def main():
    """Main entry point for the Spark consumer."""
    
    logger.info("Starting Toxicity Spark Consumer")
    logger.info(f"Configuration:")
    logger.info(f"  Kafka Bootstrap: {CONFIG['kafka_bootstrap']}")
    logger.info(f"  Kafka Topics: {CONFIG['kafka_topics']}")
    logger.info(f"  API URL: {CONFIG['api_url']}")
    logger.info(f"  Batch Timeout: {CONFIG['batch_timeout_ms']}ms")
    logger.info(f"  Checkpoint Dir: {CONFIG['checkpoint_dir']}")
    logger.info(f"  Max Retries: {CONFIG['max_retries']}")
    
    spark = None
    try:
        # Ensure checkpoint directory exists
        os.makedirs(CONFIG['checkpoint_dir'], exist_ok=True)
        
        # Create Spark session
        spark = create_spark_session()
        
        # Create Kafka stream
        kafka_df = create_kafka_stream(spark)

        # init classifier client: 
        classifier = ToxicityClassifierClient(
            base_url=CONFIG['api_url'],
            token=CONFIG.get('api_token'),
            timeout=CONFIG['request_timeout_s']
        )
        classifier.authenticate()
        
        # Write stream with batch processing
        query = kafka_df \
            .writeStream \
            .foreachBatch(
                lambda batch_df, batch_id: 
                process_batch(batch_df, batch_id, classifier)) \
            .option("checkpointLocation", CONFIG['checkpoint_dir']) \
            .trigger(processingTime=f"{CONFIG['batch_timeout_ms'] // 1000} seconds") \
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
