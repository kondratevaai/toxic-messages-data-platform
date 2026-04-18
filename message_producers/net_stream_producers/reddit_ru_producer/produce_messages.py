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

# from kafka import KafkaProducer
from message_producers.producer import ToxicityProducer
from message_producers.msg_utils import load_source_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


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
    # inst base class: 
    # cfg_path = os.getenv('SOURCE_CONFIG', '/app/source_config.json')
    reddit_producer = ToxicityProducer()

    # additional param from the config: 
    subreddits = reddit_producer.custpm_config.get('subreddits')
    user_agent = reddit_producer.custpm_config.get('user_agent', 'reddit-producer/1.0')

    seen_texts = set()
    total_sent = 0

    logger.info(f"Starting Reddit producer: subreddits={subreddits}, "
                f"partition={reddit_producer.kafka_partition}, interval={reddit_producer.custpm_config.get('fetch_interval', 5)}s")

    try:
        while True:
            for sub in subreddits:

                # get one more msg: 
                comments = fetch_subreddit_comments(sub, user_agent)
                new_count = 0
                for text in comments:
                    # deduplicate within session
                    text_hash = hash(text)
                    if text_hash in seen_texts:
                        continue
                    seen_texts.add(text_hash)

                    # send message to Kafka: 
                    reddit_producer.produce_message(text)

                    new_count += 1
                    total_sent += 1

                if new_count > 0:
                    reddit_producer.producer.flush()
                    logger.info(f"r/{sub}: sent {new_count} new comments "
                                f"(total: {total_sent})")

            # cap seen set to avoid unbounded growth
            # if len(seen_texts) > 50000:
            #     seen_texts.clear()

            time.sleep(reddit_producer.source_config.get('fetch_interval', 5))

    except (KeyboardInterrupt, Exception) as e:
        logger.info(f"Stopping reddit producer: {e}")
    finally:
        reddit_producer.producer.close()
        logger.info(f"Done. Total messages sent: {total_sent}")


main()
