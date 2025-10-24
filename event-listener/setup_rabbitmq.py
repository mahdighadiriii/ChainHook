import asyncio
import logging
import os
import sys

import aio_pika
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Debug sys.path
logger.info(f"Initial sys.path: {sys.path}")
logger.info(f"Current working directory: {os.getcwd()}")

# Add the src directory to sys.path
src_path = os.path.join(os.path.dirname(__file__), "src")
sys.path.insert(0, src_path)
logger.info(f"Added to sys.path: {src_path}")
logger.info(f"Updated sys.path: {sys.path}")

try:
    # Try importing from src package first (for IDE)
    try:
        from src.config import settings

        logger.info("Successfully imported config from src.config")
    except ImportError:
        # Fall back to direct import (for Docker with PYTHONPATH set)
        from src.config import settings

        logger.info("Successfully imported config")
except ImportError as e:
    logger.error(f"Failed to import config: {str(e)}")
    logger.error(f"Contents of current directory: {os.listdir('.')}")
    if os.path.exists("src"):
        logger.error(f"Contents of src directory: {os.listdir('src')}")
    raise

logger.info(f"Loaded RABBITMQ_URL: {settings.rabbitmq_url}")


async def setup_rabbitmq():
    """Set up RabbitMQ exchanges and queues"""
    max_retries = 15
    retry_delay = 3

    for attempt in range(max_retries):
        try:
            logger.info(
                f"Attempt {attempt + 1}/{max_retries} - Connecting to RabbitMQ: {settings.rabbitmq_url}"
            )
            connection = await aio_pika.connect_robust(
                settings.rabbitmq_url, timeout=10
            )
            async with connection:
                channel = await connection.channel()

                # Declare exchanges
                logger.info("Declaring exchanges...")
                await channel.declare_exchange(
                    "events.exchange", aio_pika.ExchangeType.TOPIC, durable=True
                )
                await channel.declare_exchange(
                    "events.dlx", aio_pika.ExchangeType.TOPIC, durable=True
                )

                # Declare queues
                logger.info("Declaring queues...")
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

                # Bind queues
                logger.info("Binding queues...")
                queue = await channel.get_queue("events.detected")
                await queue.bind(
                    exchange="events.exchange",
                    routing_key="events.*",
                )

                dlq = await channel.get_queue("events.dlq")
                await dlq.bind(exchange="events.dlx", routing_key="events.failed")

                logger.info(
                    "✅ RabbitMQ setup complete: exchanges and queues initialized"
                )
                return

        except Exception as e:
            logger.error(
                f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}", exc_info=True
            )
            if attempt + 1 == max_retries:
                logger.error("❌ Failed to set up RabbitMQ after all retries")
                raise
            logger.info(f"Retrying in {retry_delay} seconds...")
            await asyncio.sleep(retry_delay)


if __name__ == "__main__":
    asyncio.run(setup_rabbitmq())
