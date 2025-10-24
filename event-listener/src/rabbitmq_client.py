import asyncio
import json
import logging
from typing import Any, Dict

import aio_pika
import pika

from src.config import settings

logger = logging.getLogger(__name__)


class RabbitMQPublisher:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
        """Connect to RabbitMQ and setup queues"""
        try:
            self.connection = pika.BlockingConnection(
                pika.URLParameters(settings.rabbitmq_url)
            )
            self.channel = self.connection.channel()
            self.setup_queues()
            logger.info("Connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def setup_queues(self):
        """Setup exchanges, queues and DLQ"""

        self.channel.exchange_declare(
            exchange="events.exchange", exchange_type="topic", durable=True
        )

        self.channel.exchange_declare(
            exchange="events.dlx", exchange_type="topic", durable=True
        )

        self.channel.queue_declare(queue="events.dlq", durable=True)

        self.channel.queue_declare(
            queue="events.detected",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "events.dlx",
                "x-dead-letter-routing-key": "events.failed",
                "x-message-ttl": 300000,
            },
        )

        # Bind queues
        self.channel.queue_bind(
            exchange="events.exchange", queue="events.detected", routing_key="events.*"
        )

        self.channel.queue_bind(
            exchange="events.dlx", queue="events.dlq", routing_key="events.failed"
        )

    def publish(self, event_data: Dict[str, Any]):
        """Publish event to RabbitMQ"""
        try:
            if not self.connection or self.connection.is_closed:
                self.connect()

            message = json.dumps(event_data)

            self.channel.basic_publish(
                exchange="events.exchange",
                routing_key="events.blockchain",
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2, content_type="application/json"
                ),
            )

            logger.info(f"Published event: {event_data.get('event_type', 'unknown')}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            try:
                self.connect()
            except:
                pass
            return False

    def close(self):
        """Close connection"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()


publisher = None


def get_publisher():
    global publisher
    if publisher is None:
        publisher = RabbitMQPublisher()
    return publisher


async def publish_event(event_data: dict):
    """Publish event to RabbitMQ asynchronously with retries"""
    max_retries = 5
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            logger.info(
                f"Attempt {attempt + 1}/{max_retries} to connect to RabbitMQ: {settings.rabbitmq_url}"
            )
            connection = await aio_pika.connect_robust(
                settings.rabbitmq_url, timeout=10
            )
            async with connection:
                channel = await connection.channel()

                exchange = await channel.get_exchange("events.exchange")

                message = aio_pika.Message(
                    body=json.dumps(event_data).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json",
                )

                await exchange.publish(
                    message,
                    routing_key="events.blockchain",
                )

                logger.info(
                    f"Published event: {event_data.get('event_type', 'unknown')}"
                )
                return
        except Exception as e:
            logger.error(
                f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}", exc_info=True
            )
            if attempt + 1 == max_retries:
                raise
            await asyncio.sleep(retry_delay)
