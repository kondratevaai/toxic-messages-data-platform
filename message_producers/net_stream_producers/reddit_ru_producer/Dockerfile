FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python

COPY produce_messages.py .
COPY source_config.json .

ENV PYTHONUNBUFFERED=1

CMD ["python", "produce_messages.py"]
