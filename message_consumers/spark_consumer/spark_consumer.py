"""
Spark Consumer for Toxicity Detection

Reads messages from Kafka topics and sends them to the FastAPI toxicity classifier service.
Runs multiple streaming queries in parallel (one per Kafka partition) within a single
Spark session.  Each query has its own ToxicityClassifierClient; rows within a micro-batch
are classified concurrently via asyncio + ThreadPoolExecutor.
"""

import os
import sys
import json
import socket
import logging
import asyncio
import requests
import threading
import time as time_module
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, get_json_object, current_timestamp
)
from pyspark.sql.types import StringType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(processName)s] - %(message)s",
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

# Thread-safe store for recent predictions (Grafana polls this)
MAX_RECENT = 200
recent_predictions = deque(maxlen=MAX_RECENT)
predictions_lock = threading.Lock()


class MetricsHandler(BaseHTTPRequestHandler):
    """Tiny HTTP handler that serves recent predictions as JSON."""

    def do_GET(self):
        if self.path.startswith("/recent"):
            with predictions_lock:
                data = list(recent_predictions)
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


def start_metrics_server(port: int = 8081):
    """Start the metrics HTTP server in a daemon thread."""
    server = HTTPServer(("0.0.0.0", port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Metrics HTTP server started on port {port}")

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


async def _classify_row(
    text: str,
    partition: int,
    batch_id: int,
    classifier: ToxicityClassifierClient,
    consumer_cfg: dict,
    executor: ThreadPoolExecutor,
) -> bool:
    """
    Classify a single row asynchronously (runs the synchronous classifier.classify
    in a thread-pool so that multiple rows are processed concurrently).

    Returns True on success, False on failure.
    """
    loop = asyncio.get_running_loop()
    source = consumer_cfg['source']
    max_retries = CONFIG['max_retries']

    for attempt in range(1, max_retries + 1):
        try:
            result = await loop.run_in_executor(executor, classifier.classify, text)

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
            return True

        except requests.RequestException as e:
            logger.warning(
                f"Batch {batch_id}: API call attempt {attempt}/{max_retries} failed: {e}"
            )
            if attempt == max_retries:
                logger.error(
                    f"Batch {batch_id}: Failed to classify after {max_retries} attempts: "
                    f"{text[:100]}"
                )
                return False
            await asyncio.sleep(2 ** (attempt - 1))

        except Exception as e:
            logger.error(
                f"Batch {batch_id}: Unexpected error during classification: {e}"
            )
            return False

    return False


async def _process_batch_async(
    rows: list,
    batch_id: int,
    classifier: ToxicityClassifierClient,
    consumer_cfg: dict,
) -> tuple[int, int]:
    """
    Process all rows of a micro-batch concurrently.
    Returns (successful_count, failed_count).
    """
    executor = ThreadPoolExecutor(max_workers=min(len(rows), 16))
    tasks = []

    for row in rows:
        text = row["text"]
        partition = row["partition"]

        if not text or len(text.strip()) == 0:
            logger.warning(f"Batch {batch_id}: Skipping empty text")
            continue

        tasks.append(
            _classify_row(
                text, partition, batch_id,
                classifier, consumer_cfg, executor,
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)
    executor.shutdown(wait=False)

    successful = sum(1 for r in results if r is True)
    failed = len(results) - successful
    return successful, failed


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    classifier: ToxicityClassifierClient,
    consumer_cfg: dict,
) -> None:
    """
    Process a batch of messages from Kafka and send to classifier.
    All rows within the batch are classified concurrently via asyncio so that
    a slow or stuck API call on one message does not block the others.

    Args:
        batch_df: DataFrame containing the batch of messages
        batch_id: Batch identifier
        classifier: ToxicityClassifierClient instance
        consumer_cfg: Per-consumer configuration dict (contains 'source', 'name', etc.)
    """
    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id}: Empty batch, skipping")
        return

    logger.info(f"Processing batch {batch_id} with {batch_df.count()} messages")

    try:
        rows = batch_df.select("text", "partition").collect()

        successful, failed = asyncio.run(
            _process_batch_async(rows, batch_id, classifier, consumer_cfg)
        )

        logger.info(
            f"Batch {batch_id} completed: {successful} successful, {failed} failed"
        )

    except Exception as e:
        logger.error(f"Batch {batch_id}: Unexpected error in processing: {e}", exc_info=True)


def wait_for_kafka(host: str, port: int, timeout: int = 120) -> None:
    """Block until the Kafka broker is reachable (DNS + TCP)."""
    for attempt in range(1, timeout + 1):
        try:
            socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
            sock = socket.create_connection((host, port), timeout=5)
            sock.close()
            logger.info(f"Kafka broker {host}:{port} is reachable")
            return
        except (socket.gaierror, OSError) as e:
            if attempt % 10 == 0:
                logger.warning(
                    f"Waiting for Kafka at {host}:{port} "
                    f"(attempt {attempt}/{timeout}): {e}"
                )
            time_module.sleep(1)
    raise RuntimeError(f"Kafka broker {host}:{port} unreachable after {timeout}s")


def create_kafka_stream(
    spark: SparkSession, consumer_cfg: dict
) -> DataFrame:
    """
    Create a streaming DataFrame that reads from a specific Kafka partition.

    Args:
        spark: Active SparkSession.
        consumer_cfg: Per-consumer config dict (topic, partition, bootstrap, etc.).
    Returns:
        Streaming DataFrame with parsed text and metadata columns.
    """
    topic = consumer_cfg['kafka_topic']
    partition = consumer_cfg['kafka_partition']
    assign_json = json.dumps({topic: [partition]})

    logger.info(
        f"Creating Kafka stream: "
        f"bootstrap={consumer_cfg['kafka_bootstrap']} "
        f"topic={topic} partition={partition}"
    )

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", consumer_cfg['kafka_bootstrap']) \
        .option("assign", assign_json) \
        .option("includeHeaders", "true") \
        .option("failOnDataLoss", consumer_cfg['failOnDataLoss']) \
        .load()

    df = df.select(
        col("value").cast(StringType()).alias("raw_value"),
        col("timestamp").alias("kafka_timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
    )
    df = df.select(
        get_json_object(col("raw_value"), "$.text").alias("text"),
        col("kafka_timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
        current_timestamp().alias("processed_at"),
    )
    df = df.filter(col("text").isNotNull())
    return df


def main():
    """
    Main entry point.
    Creates a single Spark session and starts one streaming query per consumer
    defined in the config.  Each query reads its own Kafka partition and has
    its own ToxicityClassifierClient.  Queries run concurrently inside the
    same JVM — no multiprocessing needed.
    """
    logger.info("Starting Toxicity Spark Consumer")
    logger.info(f"  API URL: {CONFIG['api_url']}")
    logger.info(f"  Max Retries: {CONFIG['max_retries']}")
    logger.info(f"  Consumers: {[c['name'] for c in CONFIG['consumers']]}")

    # Metrics HTTP server (polled by Grafana)
    start_metrics_server(port=CONFIG.get('metrics_port', 8081))

    # Wait for Kafka to be DNS-resolvable and accepting TCP connections
    bootstrap = CONFIG['consumers'][0]['kafka_bootstrap']
    host, port_str = bootstrap.split(':')
    wait_for_kafka(host, int(port_str))

    spark = None
    classifiers: list[ToxicityClassifierClient] = []
    try:
        # Single Spark session for the entire process
        spark = SparkSession.builder \
            .appName("ToxicitySparkConsumer") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark session created: {spark.version}")

        # Start one streaming query per consumer config
        queries = []
        for consumer_cfg in CONFIG['consumers']:
            name = consumer_cfg['name']

            # Each query gets its own API client
            classifier = ToxicityClassifierClient(
                base_url=CONFIG['api_url'],
                token=CONFIG.get('api_token'),
                timeout=CONFIG['request_timeout_s'],
            )
            classifier.authenticate()
            classifiers.append(classifier)

            # Kafka stream for this consumer's partition
            kafka_df = create_kafka_stream(spark, consumer_cfg)

            # Checkpoint directory
            os.makedirs(consumer_cfg['checkpoint_dir'], exist_ok=True)

            # Capture classifier & consumer_cfg in the lambda via default args
            query = kafka_df.writeStream \
                .foreachBatch(
                    lambda batch_df, batch_id, _clf=classifier, _cfg=consumer_cfg:
                        process_batch(batch_df, batch_id, _clf, _cfg)
                ) \
                .option("checkpointLocation", consumer_cfg['checkpoint_dir']) \
                .trigger(processingTime=f"{consumer_cfg['batch_timeout_ms'] // 1000} seconds") \
                .queryName(name) \
                .start()

            queries.append(query)
            logger.info(f"Streaming query '{name}' started")

        logger.info(f"All {len(queries)} streaming queries running, waiting for data...")

        # awaitAnyTermination blocks until any query stops (or forever)
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error in consumer: {e}", exc_info=True)
    finally:
        for clf in classifiers:
            clf.close()
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
