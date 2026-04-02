FROM python:3.11-slim

# Install system dependencies - only JRE is needed for PySpark
RUN apt-get update && apt-get install -y default-jre-headless && rm -rf /var/lib/apt/lists/*

# Create working directory
WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    requests==2.31.0 \
    kafka-python==2.0.2 \
    python-dotenv==1.0.0

# Pre-download the Spark Kafka connector JAR into the Ivy cache at build time
RUN spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --conf spark.driver.extraJavaOptions="-Divy.home=/root/.ivy2" \
    /dev/null 2>/dev/null; exit 0

# Copy consumer script
COPY spark_consumer.py /app/

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SPARK_LOCAL_IP=127.0.0.1

# Run via spark-submit so the Kafka package is properly loaded into the JVM classpath
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--conf", "spark.driver.extraJavaOptions=-Divy.home=/root/.ivy2", \
     "/app/spark_consumer.py"]
