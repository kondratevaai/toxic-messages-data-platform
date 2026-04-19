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
