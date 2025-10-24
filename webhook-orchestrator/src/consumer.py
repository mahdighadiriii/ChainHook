import asyncio
import json
import logging

import aio_pika

from .config import settings
from .database import get_all_webhooks, get_db_session
from .webhook_delivery import WebhookDelivery

logger = logging.getLogger(__name__)


class WebhookConsumer:
    def __init__(self):
        self.delivery = WebhookDelivery()

    async def process_event(self, event_data: dict):
        """
        Process incoming event and deliver to matching webhooks
        """
        event_type = event_data.get("event_type")
        contract_id = event_data.get("contract_id")

        logger.info(f"📨 Processing event: {event_type} from {contract_id}")

        with get_db_session() as session:
            webhooks = get_all_webhooks(session, active_only=True)

        if not webhooks:
            logger.warning("⚠️ No active webhooks found")
            return

        matching_webhooks = []
        for webhook in webhooks:
            if event_type not in webhook.event_types:
                continue

            if webhook.contract_id and webhook.contract_id != contract_id:
                continue

            matching_webhooks.append(
                {
                    "id": webhook.id,
                    "url": webhook.url,
                    "secret": webhook.secret,
                    "headers": webhook.headers,
                }
            )

        if not matching_webhooks:
            logger.info(f"ℹ️ No matching webhooks for {event_type} from {contract_id}")
            return

        logger.info(f"🎯 Found {len(matching_webhooks)} matching webhook(s)")

        tasks = [
            self.delivery.deliver(webhook, event_data) for webhook in matching_webhooks
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = sum(1 for r in results if r is True)
        logger.info(
            f"📊 Delivery results: {success_count}/{len(matching_webhooks)} successful"
        )

    async def start(self):
        """Start consuming events from RabbitMQ"""
        max_retries = 15
        retry_delay = 3

        while True:
            for attempt in range(max_retries):
                try:
                    logger.info(
                        f"Attempt {attempt + 1}/{max_retries} to connect to RabbitMQ"
                    )
                    connection = await aio_pika.connect_robust(
                        settings.rabbitmq_url, timeout=10
                    )

                    async with connection:
                        channel = await connection.channel()
                        await channel.set_qos(prefetch_count=1)

                        queue = await channel.declare_queue(
                            "events.detected",
                            durable=True,
                            arguments={
                                "x-message-ttl": 300000,
                                "x-dead-letter-exchange": "events.dlx",
                                "x-dead-letter-routing-key": "events.failed",
                            }
                        )

                        logger.info("✅ Webhook consumer ready. Waiting for events...")

                        async with queue.iterator() as queue_iter:
                            async for message in queue_iter:
                                async with message.process():
                                    try:
                                        event_data = json.loads(message.body.decode())
                                        await self.process_event(event_data)
                                    except json.JSONDecodeError:
                                        logger.error("Invalid JSON in message")
                                    except Exception as e:
                                        logger.error(
                                            f"Error processing event: {str(e)}",
                                            exc_info=True,
                                        )

                except Exception as e:
                    logger.error(
                        f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}",
                        exc_info=True,
                    )
                    if attempt + 1 == max_retries:
                        logger.error("Max retries reached. Retrying after delay...")
                        await asyncio.sleep(retry_delay * 2)
                    else:
                        await asyncio.sleep(retry_delay)
