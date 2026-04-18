FROM python:3.11-slim

WORKDIR /app
COPY . .

# Install system dependencies
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     && rm -rf /var/lib/apt/lists/*

# Install Python dependencies for S3 and Kafka
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    boto3 \
    kafka-python \
    pandas \
    PyYAML \
    python-dotenv

ENV PYTHONPATH=/app

# Copy producer script and config
# RUN mkdir -p /app/shared
# ENV PYTHONPATH=/app

# Create shared directory and copy shared resources
# RUN mkdir -p /app/shared
# COPY ../../toxic-messages-handling-project/src/config.json /app/shared/config.json
# COPY ../../toxic-messages-handling-project/src/services/utils.py /app/shared/utils.py
# COPY ../../toxic-messages-handling-project/src/services/__init__.py /app/shared/__init__.py

# Set default environment variable
# ENV MODEL_CONFIG=/app/shared/config.json

# registering all producers: 
# CMD ["python", "message_producers/s3_csv_producer/produce_messages.py"]
