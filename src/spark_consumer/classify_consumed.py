"""Main entry point for the Spark consumer."""


import logging
import os
# import sys
# import json
import requests
import threading
from collections import deque
# from http.server import HTTPServer, BaseHTTPRequestHandler
# from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame

from http.server import HTTPServer
from utils import load_source_config
# from logger import logger
from services.toxicity_client import ToxicityClassifierClient
# from src.logger import setup_logger
from logger import setup_logger
# from services.metrics_handler import MetricsHandler
from services.spark_consumer import SparkConsumer

import json
# import requests
import logging
import threading
# from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler


setup_logger()
logger = logging.getLogger(__name__)


class MetricsHandler(BaseHTTPRequestHandler):
    """Tiny HTTP handler that serves recent predictions as JSON."""

    def __init__(self, *args, recent_predictions=None, predictions_lock=None, **kwargs):
        self.recent_predictions = recent_predictions
        self.predictions_lock = predictions_lock
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path.startswith("/recent"):
            with self.predictions_lock:
                data = list(self.recent_predictions)
            payload = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(payload)
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress access logs


def start_metrics_server(port: int = 8081, predictions_lock=None, recent_predictions=None):
    """Start the metrics HTTP server in a daemon thread."""

    # Create a custom handler with the required arguments
    handler = lambda *args, **kwargs: MetricsHandler(
        *args,
        recent_predictions=recent_predictions,
        predictions_lock=predictions_lock,
        **kwargs
    )

    # start the HTTP server: 
    server = HTTPServer(("0.0.0.0", port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Metrics HTTP server started on port {port}")

# def start_metrics_server(port: int = 8081):
#     """Start the metrics HTTP server in a daemon thread."""
    # server = HTTPServer(("0.0.0.0", port), MetricsHandler)
    # thread = threading.Thread(target=server.serve_forever, daemon=True)
    # thread.start()
    # logger.info(f"Metrics HTTP server started on port {port}")

    


class MessagesProcessor:
    """
    1. Init Toxicity API client 
    2. Create SparkSession to consume messages from Kafka
    3. Read messages in batches, send to classifier, store results in thread-safe deque for Grafana
    4. Implement retry logic for failed API calls
    """

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = load_source_config(config_path)
        self.predictions_lock = threading.Lock()
        self.recent_predictions = deque(maxlen=self.config.get('n_displayed', 200))

    def start_metrics_handler(self, port: int = 8081) -> None:
        """Start the metrics HTTP server for Grafana."""
        start_metrics_server(
            port=port,
            predictions_lock=self.predictions_lock,
            recent_predictions=self.recent_predictions
        )

    def authenticate_classifier(self) -> ToxicityClassifierClient:
        self.classifier = ToxicityClassifierClient(
            base_url=self.config['api_url'],
            token=self.config.get('api_token'),
            timeout=self.config['request_timeout_s']
        )
        self.classifier.authenticate()

    def create_spark_consumer(self) -> SparkConsumer:
        self.consumer: SparkConsumer = SparkConsumer(self.config_path)
        self.consumer.create_spark_session()
        return self.consumer

    # def fetch_topics_and_partitions(self):
    #     """Fetch all topics and their partitions using SparkConsumer."""
    #     topics_partitions = self.consumer.get_topics_and_partitions()
    #     return topics_partitions

    def process_batch(
            self,
            batch_df: DataFrame, 
            batch_id: int, 
            # classifier: ToxicityClassifierClient
            ) -> None:
        
        if batch_df.count() == 0:
            logger.info(f"Batch {batch_id}: Empty batch, skipping")
            return
        
        logger.info(f"Processing batch {batch_id} with {batch_df.count()} messages")
        
        try:
            
            # Collect texts with partition info from the batch
            rows = batch_df.select("text", "partition", "topic").collect()
            
            successful = 0
            failed = 0
            
            for row in rows:
                text = row["text"]
                partition = row["partition"]
                topic = row["topic"]

                if not text or len(text.strip()) == 0:
                    logger.warning(f"Batch {batch_id}: Skipping empty text")
                    failed += 1
                    continue
                
                # Retry logic for failed requests
                for attempt in range(1, self.config['max_retries'] + 1):
                    try:
                        result = self.classifier.classify(text)
                        
                        prediction_entry = {
                            "text": text[:200],
                            "partition": partition,
                            "topic": topic,
                            "prediction": result.get('prediction'),
                            "prediction_label": "toxic" if result.get('prediction') == 1 else "non_toxic",
                            "processing_time_ms": result.get('processing_time_ms'),
                            "timestamp": datetime.utcnow().isoformat(),
                            "batch_id": batch_id
                        }
                        with self.predictions_lock:
                            self.last_predictions.appendleft(prediction_entry)

                        logger.info(
                            f"Text | {text[:100]} | "
                            f"Batch {batch_id}: Text classified | "
                            f"Topic: {topic} | Partition: {partition} | "
                            f"Prediction: {result.get('prediction')} | "
                            f"Time: {result.get('processing_time_ms', 'N/A')}ms"
                        )
                        successful += 1
                        break
                        
                    except requests.RequestException as e:
                        mr = self.config['max_retries']
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
            
            self.classifier.close()
            
            logger.info(
                f"Batch {batch_id} completed: {successful} successful, {failed} failed"
            )
            
        except Exception as e:
            logger.error(f"Batch {batch_id}: Unexpected error in processing: {e}", exc_info=True)

    def start_processing(self) -> None:
        """Main method to start Spark streaming and process messages."""
        try:
            # Start metrics HTTP server for Grafana
            self.start_metrics_handler(port=8081)

            # Create Spark consumer
            self.create_spark_consumer()

            # Authenticate classifier client
            self.authenticate_classifier()

            # Create Kafka stream
            kafka_df = self.consumer.create_kafka_stream()

            # Write stream with batch processing
            query = kafka_df \
                .writeStream \
                .foreachBatch(
                    lambda batch_df, batch_id: 
                    self.process_batch(batch_df, batch_id)) \
                .option("checkpointLocation", self.config['checkpoint_dir']) \
                .trigger(
                    processingTime=f"{self.config['batch_timeout_ms'] // 1000} seconds"
                ) \
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
        # finally:
            # if self.consumer is not None:
            #     self.consumer.stop()
            #     logger.info("Spark consumer stopped")


# logger = logging.getLogger(__name__)

def main():
    
    logger.info("Starting Toxicity Spark Consumer")

    # load config: 
    cfg_path = os.getenv('PROCESSOR_CONFIG', '/app/processor_config.json')

    # start processing: 
    processor = MessagesProcessor(cfg_path)
    processor.start_processing()


if __name__ == "__main__":
    main()
