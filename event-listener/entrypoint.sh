#!/bin/bash
set -e

echo "Setting up RabbitMQ..."
python /app/event-listener/setup_rabbitmq.py

echo "Starting event consumer..."
exec python -m src.consumer
