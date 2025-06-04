# # Dockerfile (for Kafka producer/consumer)

# FROM python:3.9-slim

# WORKDIR /scripts

# COPY scripts/requirements.txt ./requirements.txt
# RUN pip install -r requirements.txt

# COPY scripts/ ./scripts
# COPY data/ /data
# Dockerfile (for Kafka producer and consumer)

FROM python:3.9-slim

WORKDIR /scripts

COPY scripts/requirements.txt .
RUN pip install -r requirements.txt

COPY scripts/ .
COPY data/ /data