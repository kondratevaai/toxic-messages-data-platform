# import os
# import sys
import json
# import requests
import logging
import threading
# from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler

from logger import setup_logger
# from typing import Optional
# from datetime import datetime
# from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql.functions import (
#     col, from_json, get_json_object, window, count, current_timestamp
# )
# from pyspark.sql.types import StructType, StructField, StringType
# from utils import load_source_config
# from logger import logger


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

    def start_metrics_server(self, port: int = 8081):
        """Start the metrics HTTP server in a daemon thread."""
        server = HTTPServer(
            ("0.0.0.0", port), 
            lambda *args, 
            **kwargs: MetricsHandler(*args, recent_predictions=self.recent_predictions, predictions_lock=self.predictions_lock, **kwargs))
        
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        logger.info(f"Metrics HTTP server started on port {port}")
