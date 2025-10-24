#!/bin/bash
set -e

echo "🚀 Starting Webhook Orchestrator..."

echo "⏳ Waiting for PostgreSQL..."
until python -c "import psycopg2; psycopg2.connect('$POSTGRES_URL')" 2>/dev/null; do
  sleep 2
done
echo "✅ PostgreSQL is ready"

echo "⏳ Waiting for Redis..."
until python -c "import redis; redis.Redis.from_url('$REDIS_URL').ping()" 2>/dev/null; do
  sleep 2
done
echo "✅ Redis is ready"

echo "⏳ Waiting for RabbitMQ..."
sleep 10

echo "🎯 Setting up RabbitMQ..."
python -m webhook_orchestrator.setup_rabbitmq

echo "🚀 Starting Webhook Orchestrator service..."
exec "$@"