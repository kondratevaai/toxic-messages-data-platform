FROM python:3.11-slim

# Install system dependencies - JRE for PySpark, curl for downloading JARs
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create working directory
WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    requests==2.31.0 \
    kafka-python==2.0.2 \
    python-dotenv==1.0.0

# Download Spark Kafka connector JARs directly into PySpark's jars directory.
# This is more reliable than --packages which requires Ivy resolution at runtime.
ENV SPARK_JARS_DIR=/usr/local/lib/python3.11/site-packages/pyspark/jars
RUN curl -fSL -o ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fSL -o ${SPARK_JARS_DIR}/kafka-clients-3.4.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fSL -o ${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fSL -o ${SPARK_JARS_DIR}/commons-pool2-2.12.0.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

COPY spark_consumer.py /app/
COPY consumer_config.json /app/

ENV PYTHONUNBUFFERED=1

CMD ["spark-submit", "/app/spark_consumer.py"]
