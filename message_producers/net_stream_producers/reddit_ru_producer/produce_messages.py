"""
Reddit RU Producer
==================
Fetches recent comments from Russian-language subreddits and pushes them
to Kafka topic partition 1. Uses Reddit's public JSON API (no auth required).
"""

import os
import sys
import json
import time
import logging
import urllib.request
import urllib.error

from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def load_source_config(config_path: str) -> dict:
    with open(config_path, 'r') as f:
        return json.load(f)


SOURCE_CONFIG_PATH = os.getenv('SOURCE_CONFIG', '/app/source_config.json')
SOURCE_CONFIG = load_source_config(SOURCE_CONFIG_PATH)


def create_kafka_producer(retries=None, delay=None):
    if retries is None:
        retries = SOURCE_CONFIG.get('kafka_retries', 5)
    if delay is None:
        delay = SOURCE_CONFIG.get('kafka_retry_delay', 5)
    kafka_bootstrap = SOURCE_CONFIG['kafka_bootstrap']
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            )
            logger.info(f"Connected to Kafka at {kafka_bootstrap}")
            return producer
        except Exception as e:
            logger.warning(f"Kafka attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
    raise ConnectionError(f"Could not connect to Kafka after {retries} attempts")


def fetch_subreddit_comments(subreddit: str, user_agent: str, limit: int = 25) -> list:
    """
    Fetch recent comments from a subreddit using Reddit's public JSON API.
    No authentication needed for read-only access.
    """
    url = f"https://www.reddit.com/r/{subreddit}/comments.json?limit={limit}"
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))

        comments = []
        for child in data.get("data", {}).get("children", []):
            body = child.get("data", {}).get("body", "")
            if body and body.strip() and body != "[deleted]" and body != "[removed]":
                comments.append(body.strip())
        return comments
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as e:
        logger.warning(f"Failed to fetch r/{subreddit}: {e}")
        return []


def main():
    producer = create_kafka_producer()

    kafka_topic = SOURCE_CONFIG['kafka_topic']
    kafka_partition = SOURCE_CONFIG['kafka_partition']
    subreddits = SOURCE_CONFIG['subreddits']
    fetch_interval = SOURCE_CONFIG['fetch_interval_s']
    user_agent = SOURCE_CONFIG.get('user_agent', 'reddit-producer/1.0')

    seen_texts = set()
    total_sent = 0

    logger.info(f"Starting Reddit producer: subreddits={subreddits}, "
                f"partition={kafka_partition}, interval={fetch_interval}s")

    try:
        while True:
            for sub in subreddits:
                comments = fetch_subreddit_comments(sub, user_agent)
                new_count = 0
                for text in comments:
                    # deduplicate within session
                    text_hash = hash(text)
                    if text_hash in seen_texts:
                        continue
                    seen_texts.add(text_hash)

                    producer.send(kafka_topic, value={'text': text},
                                  partition=kafka_partition)
                    new_count += 1
                    total_sent += 1

                if new_count > 0:
                    producer.flush()
                    logger.info(f"r/{sub}: sent {new_count} new comments "
                                f"(total: {total_sent})")

            # cap seen set to avoid unbounded growth
            if len(seen_texts) > 50000:
                seen_texts.clear()

            time.sleep(fetch_interval)

    except (KeyboardInterrupt, Exception) as e:
        logger.info(f"Stopping reddit producer: {e}")
    finally:
        producer.close()
        logger.info(f"Done. Total messages sent: {total_sent}")


main()
