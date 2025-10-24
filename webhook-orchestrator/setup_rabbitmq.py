import asyncio
import logging
import os
import sys

import aio_pika
from dotenv import load_dotenv
from src.config import settings

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


async def setup_rabbitmq():
    """Ensure RabbitMQ exchanges and queues exist"""
    max_retries = 15
    retry_delay = 3

    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_retries} - Connecting to RabbitMQ")
            connection = await aio_pika.connect_robust(
                settings.rabbitmq_url, timeout=10
            )
            async with connection:
                channel = await connection.channel()

                await channel.declare_exchange(
                    "events.exchange", aio_pika.ExchangeType.TOPIC, durable=True
                )
                await channel.declare_exchange(
                    "events.dlx", aio_pika.ExchangeType.TOPIC, durable=True
                )

                await channel.declare_queue(
                    "events.detected",
                    durable=True,
                    arguments={
                        "x-dead-letter-exchange": "events.dlx",
                        "x-dead-letter-routing-key": "events.failed",
                        "x-message-ttl": 300000,
                    },
                )
                await channel.declare_queue("events.dlq", durable=True)

                queue = await channel.get_queue("events.detected")
                await queue.bind(
                    exchange="events.exchange",
                    routing_key="events.*",
                )

                dlq = await channel.get_queue("events.dlq")
                await dlq.bind(exchange="events.dlx", routing_key="events.failed")

                logger.info("✅ RabbitMQ setup complete for webhook orchestrator")
                return

        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            if attempt + 1 == max_retries:
                logger.error("❌ Failed to set up RabbitMQ after all retries")
                raise
            await asyncio.sleep(retry_delay)


if __name__ == "__main__":
    asyncio.run(setup_rabbitmq())
