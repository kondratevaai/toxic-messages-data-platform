import os
import sys
import json
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
from utils import load_source_config
from logger import logger


CONFIG_PATH = os.getenv('CONSUMER_CONFIG', '/app/consumer_config.json')
CONFIG = load_source_config(CONFIG_PATH)

# logger = logging.getLogger(__name__)

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
        # Start metrics HTTP server for Grafana
        start_metrics_server(port=8081)

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
