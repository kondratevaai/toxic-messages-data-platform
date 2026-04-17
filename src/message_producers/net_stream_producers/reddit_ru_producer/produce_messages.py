"""
Reddit RU Producer
==================
Fetches recent comments from Russian-language subreddits and pushes them
to Kafka topic partition 1. Uses Reddit's public JSON API (no auth required).
"""

import os
import sys
import json
import logging

import aiohttp
import asyncio

from src.message_producers.utils import load_source_config
from src.message_producers.producer import ToxMEssagesProducer
# from kafka import KafkaProducer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


SOURCE_CONFIG_PATH = os.getenv('SOURCE_CONFIG', '/app/source_config.json')
SOURCE_CONFIG = load_source_config(SOURCE_CONFIG_PATH)


async def fetch_subreddit_comments(subreddit: str, user_agent: str, limit: int = 25) -> list:
    """
    Fetch recent comments from a subreddit using Reddit's public JSON API.
    No authentication needed for read-only access.
    Async version using aiohttp.
    """
    url = f"https://www.reddit.com/r/{subreddit}/comments.json?limit={limit}"
    headers = {"User-Agent": user_agent}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=15) as resp:
                if resp.status != 200:
                    logger.warning(f"Failed to fetch r/{subreddit}: HTTP {resp.status}")
                    return []
                data = await resp.json()

        comments = []
        for child in data.get("data", {}).get("children", []):
            body = child.get("data", {}).get("body", "")
            if body and body.strip() and body != "[deleted]" and body != "[removed]":
                comments.append(body.strip())
        return comments
    except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
        logger.warning(f"Failed to fetch r/{subreddit}: {e}")
        return []


def start_producer():
    # producer = create_kafka_producer()
    # kafka_topic = SOURCE_CONFIG['kafka_topic']
    # kafka_partition = SOURCE_CONFIG['kafka_partition']

    subreddits = SOURCE_CONFIG['subreddits']
    fetch_interval = SOURCE_CONFIG['fetch_interval_s']
    user_agent = SOURCE_CONFIG.get('user_agent', 'reddit-producer/1.0')

    # producer = 
    
    
if __name__ == "__main__":
    start_producer()
