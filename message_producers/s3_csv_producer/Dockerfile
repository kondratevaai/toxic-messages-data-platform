FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies for S3 and Kafka
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    boto3 \
    kafka-python \
    pandas \
    PyYAML \
    python-dotenv

# Copy producer script and config
COPY produce_messages.py .
COPY source_config.json .

# Create shared directory and copy shared resources
# RUN mkdir -p /app/shared
# COPY ../../toxic-messages-handling-project/src/config.json /app/shared/config.json
# COPY ../../toxic-messages-handling-project/src/services/utils.py /app/shared/utils.py
# COPY ../../toxic-messages-handling-project/src/services/__init__.py /app/shared/__init__.py

# Set default environment variable
ENV MODEL_CONFIG=/app/shared/config.json

# Run producer
CMD ["python", "produce_messages.py"]
