FROM python:3.12-slim

WORKDIR /app

# confluent-kafka needs librdkafka
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "-u", "producer.py"]
