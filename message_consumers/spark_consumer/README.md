# Spark Consumer for Toxicity Classification

A production-ready Apache Spark Structured Streaming consumer that reads messages from Kafka topics and sends them to a FastAPI toxicity classifier service for real-time toxicity detection.

## Overview

The Spark Consumer:
- **Reads** messages from Kafka topics in real-time
- **Parses** JSON-formatted messages containing text
- **Batches** messages using Spark Structured Streaming
- **Classifies** text samples using the FastAPI toxicity classifier
- **Handles errors** with exponential backoff retry logic
- **Maintains checkpoints** for fault tolerance

## Architecture

```
Kafka Topic (toxic-messages)
         ↓
  Spark Structured Streaming
         ↓
  Batch Processing (Foreachbatch)
         ↓
  HTTP Requests to FastAPI /forward endpoint
         ↓
  Toxicity Predictions (logged and stored)
```

## Configuration

### Environment Variables

Configure behavior through environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka bootstrap server address |
| `KAFKA_TOPICS` | `toxic-messages` | Comma-separated Kafka topics to consume |
| `API_URL` | `http://backend-api:8000` | FastAPI service base URL |
| `API_TOKEN` | (none) | Optional JWT/Bearer token for API authentication |
| `BATCH_TIMEOUT_MS` | `10000` | Micro-batch trigger interval (milliseconds) |
| `CHECKPOINT_DIR` | `/tmp/spark_consumer_checkpoint` | Spark streaming checkpoint directory |
| `MAX_RETRIES` | `3` | Number of retries for failed API calls |
| `REQUEST_TIMEOUT_S` | `30` | HTTP request timeout (seconds) |

### Example .env File

```bash
KAFKA_BOOTSTRAP=kafka:9092
KAFKA_TOPICS=toxic-messages
API_URL=http://backend-api:8000
BATCH_TIMEOUT_MS=10000
CHECKPOINT_DIR=/tmp/spark_consumer_checkpoint
MAX_RETRIES=3
REQUEST_TIMEOUT_S=30
```

## Running with Docker Compose

### 1. Start All Services

```bash
# Start the entire stack (Kafka, producers, API, consumer)
docker-compose up -d

# View logs for the spark consumer
docker-compose logs -f spark-consumer
```

### 2. Start Individual Component

```bash
# Only start the consumer (assuming Kafka and API are running)
docker-compose up -d spark-consumer
```

### 3. Stop Services

```bash
docker-compose down

# Stop and remove volumes (clears checkpoint data)
docker-compose down -v
```

## Running Locally (Development)

For local development without Docker:

### Prerequisites

```bash
# Install PySpark and dependencies
pip install pyspark==3.5.0 kafka-python==2.0.2 requests==2.31.0 python-dotenv==1.0.0
```

### Run Consumer

```bash
# Set environment variables
export KAFKA_BOOTSTRAP=localhost:9092
export API_URL=http://localhost:8000

# Run the consumer
python spark_consumer.py
```

## Input Message Format

Messages in the Kafka topic must follow this JSON structure:

```json
{
  "text": "The message text to be classified for toxicity"
}
```

### Example Messages

```json
{"text": "This is a normal message"}
{"text": "This is an offensive message"}
{"text": "Another message for classification"}
```

## Output/Response Handling

For each message, the consumer:

1. **Sends** the text to `POST /forward` endpoint
2. **Receives** JSON response with prediction:
   ```json
   {
     "id": 123,
     "user_id": 1,
     "text_raw": "original text",
     "prediction": 0,
     "processing_time_ms": 45.23,
     "text_length": 20,
     "timestamp": "2025-04-01T12:30:45"
   }
   ```
3. **Logs** the result with prediction score

### Prediction Values
- `0`: Non-toxic
- `1`: Toxic

## Fault Tolerance

### Checkpoint Management

Spark Structured Streaming maintains state using checkpoints:

- Location: `/tmp/spark_consumer_checkpoint`
- Purpose: Ensures exactly-once processing semantics
- Persistence: Survives application crashes and restarts

### Retry Logic

- **Failed API calls**: Exponential backoff (2^attempt seconds)
- **Max retries**: Configurable (default: 3)
- **Timeout handling**: Graceful failure with logging

### Error Handling

All errors are logged with context:
- Empty/null text filtering
- Connection failures
- Timeout handling
- Invalid JSON parsing
- API errors (HTTP 4xx/5xx responses)

## Logging

Logs are written to:
- **Console**: Real-time monitoring via `docker-compose logs`
- **File**: `/tmp/spark_consumer.log` (inside container)

Log levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`

Example log output:
```
2025-04-01 12:30:45 - INFO - Processing batch 1 with 100 messages
2025-04-01 12:30:48 - INFO - Batch 1 completed: 98 successful, 2 failed
2025-04-01 12:31:05 - INFO - Processing batch 2 with 95 messages
```

## API Authentication

If the FastAPI service requires authentication:

### With Bearer Token

```bash
# Set token as environment variable
export API_TOKEN="eyJhbGciOiJIUzI1NiIs..."
docker-compose up -d spark-consumer
```

### Token format

The consumer automatically handles:
- Plain JWT tokens: Adds `Bearer` prefix
- `Bearer {token}` format: Uses as-is
- No token: Unauthenticated requests

## Performance Tuning

### Adjust for Throughput

```bash
# Increase batch size and reduce frequency
BATCH_TIMEOUT_MS=30000          # Process every 30s instead of 10s
MAX_RETRIES=5                   # More tolerance for transient failures
```

### Adjust for Latency

```bash
# Decrease batch size and increase frequency
BATCH_TIMEOUT_MS=2000           # Process every 2s for lower latency
```

### Resource Configuration

Edit Dockerfile or pass Spark configs:

```dockerfile
# Add to Spark session config for more resources
.config("spark.executor.memory", "2g") \
.config("spark.executor.cores", "2") \
```

## Monitoring

### Spark UI

Spark Structured Streaming provides monitoring via Spark UI (port 4040):

```
http://localhost:4040
```

### Kafka UI

Monitor Kafka topics and consumer groups:

```
http://localhost:8080
```

Monitor the `toxic-messages` topic for message flow.

### Health Checks

```bash
# Check if consumer is running
docker-compose ps spark-consumer

# View recent logs
docker-compose logs --tail 50 spark-consumer

# Check Kafka topic
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --topic toxic-messages --describe
```

## Troubleshooting

### Consumer Not Starting

```bash
# Check logs
docker-compose logs spark-consumer

# Verify Kafka is healthy
docker-compose logs kafka

# Verify API is accessible
curl http://backend-api:8000/docs
```

### Messages Not Being Processed

```bash
# Check if messages exist in Kafka
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic toxic-messages \
  --from-beginning \
  --max-messages 5

# Verify API endpoint is working
curl -X POST http://localhost:8000/forward \
  -H "Content-Type: application/json" \
  -d '{"text_raw": "test message"}'
```

### High Error Rate

1. Check API logs: `docker-compose logs backend-api`
2. Increase `REQUEST_TIMEOUT_S`
3. Check network connectivity
4. Verify message format in Kafka

### Checkpoint Issues

```bash
# Clear checkpoint to restart from latest offset
docker-compose down -v
docker-compose up -d spark-consumer
```

## Production Deployment

For production use:

1. **Enable authentication**: Set `API_TOKEN` environment variable
2. **Use persistent storage**: Map checkpoint directory to persistent volume
3. **Configure retry policy**: Adjust `MAX_RETRIES` and timeouts
4. **Set resource limits**: Configure Spark executor memory/cores
5. **Monitor closely**: Use logging aggregation (ELK, Splunk, etc.)
6. **Scale horizontally**: Deploy multiple consumer instances for different topic partitions

## Development

### Project Structure

```
message_consumers/spark_consumer/
├── spark_consumer.py          # Main consumer script
├── Dockerfile                 # Container image definition
├── .env.example              # Example environment variables
└── README.md                 # This file
```

### Testing

To test with sample data:

```bash
# In one terminal, run producer
docker-compose up messages-producer

# In another terminal, run consumer
docker-compose up spark-consumer

# Monitor in third terminal
docker-compose logs -f kafka-ui
```

## License

Part of the Toxic Messages Data Platform project.

## Contributors

- Development Team
- Data Science Team
