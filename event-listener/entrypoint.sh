#!/bin/bash
set -e

echo "Setting up RabbitMQ..."

# Set Python path to include the event-listener directory
export PYTHONPATH="/app/event-listener:$PYTHONPATH"

# Change to the event-listener directory
cd /app/event-listener

# Run setup script
python setup_rabbitmq.py

echo "Starting event consumer..."
# Run from event-listener directory, not src
exec python -m src.consumer