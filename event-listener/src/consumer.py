import json
import logging
import time
from typing import Callable

import pika

from src.config import settings
from src.database import get_db_session, save_event

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_with_retry(
    url: str, retries: int = 15, delay: int = 3
) -> pika.BlockingConnection:
    """Connect to RabbitMQ with retry"""
    for i in range(retries):
        try:
            connection = pika.BlockingConnection(pika.URLParameters(url))
            logger.info("Connected to RabbitMQ!")
            return connection
        except Exception as e:
            logger.warning(f"RabbitMQ not ready (attempt {i + 1}/{retries}): {e}")
            time.sleep(delay)
    raise Exception("Failed to connect to RabbitMQ after retries")


class EventConsumer:
    def __init__(self):
        # Connect with retry
        self.connection = connect_with_retry(settings.rabbitmq_url)
        self.channel = self.connection.channel()

        # Declare queue (idempotent)
        self.channel.queue_declare(
            queue="events.detected",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "events.dlx",
                "x-dead-letter-routing-key": "events.failed",
                "x-message-ttl": 300000,  # 5 min
            },
        )

        self.channel.basic_qos(prefetch_count=1)
        logger.info("Consumer ready. Queue: events.detected")

    def process_message(self, ch, method, properties, body):
        """Process one message"""
        try:
            event = json.loads(body)
            logger.info(
                f"Received event: {event.get('event_type')} from {event.get('contract_id')}"
            )

            with get_db_session() as session:
                save_event(
                    session,
                    contract_id=event.get("contract_id"),
                    event_type=event.get("event_type"),
                    data=event.get("data", {}),
                )
            logger.info("Event saved to DB")

            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Message ACKed")

        except json.JSONDecodeError:
            logger.error("Invalid JSON. NACK and drop.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        except Exception as e:
            logger.error(f"Error processing event: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            # Goes to DLQ

    def start(self):
        """Start consuming"""
        logger.info("Starting consumer... Waiting for messages.")
        self.channel.basic_consume(
            queue="events.detected",
            on_message_callback=self.process_message,
            auto_ack=False,
        )
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
            self.channel.stop_consuming()
            self.connection.close()
            logger.info("Consumer stopped.")


if __name__ == "__main__":
    consumer = EventConsumer()
    consumer.start()
