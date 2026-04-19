#!/bin/bash

# the main addres for server to work: 
KAFKA_BOOTSTRAP=localhost:9092

# conf vars: 
S3_TOPIC_NAME=s3-source
INTERNET_TOPIC_NAME=internet-sources

# NOTE: both topics are for different purposes,
# the first one is for already stored s3 source => we do not need large TTL,
# the second one is for internet sources => store them for a longer time:
# KAFKA_CREATE_TOPICS: "s3-source:1:1,internet-sources:2:1"

# 1 hour: 
S3_RETENTION_MS=3600000 
# 7 days: 
INTERNET_RETENTION_MS=604800000

# Set topic-specific configurations
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --alter --topic $S3_TOPIC_NAME --config retention.ms=$S3_RETENTION_MS
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --alter --topic $INTERNET_TOPIC_NAME --config retention.ms=$INTERNET_RETENTION_MS