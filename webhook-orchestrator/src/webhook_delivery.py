import asyncio
import logging

import httpx

from .config import settings
from .database import create_webhook_log, get_db_session
from .hmac_utils import generate_hmac_signature

logger = logging.getLogger(__name__)


class WebhookDelivery:
    def __init__(self):
        self.max_retries = settings.max_retry_attempts
        self.backoff_base = settings.retry_backoff_base
        self.timeout = settings.webhook_timeout

    async def deliver(self, webhook: dict, event_data: dict) -> bool:
        """
        Deliver event to webhook with exponential backoff retry
        Returns True if successful, False otherwise
        """
        webhook_id = webhook["id"]
        url = webhook["url"]
        secret = webhook["secret"]
        custom_headers = webhook.get("headers", {})

        payload = {
            "event": event_data,
            "webhook_id": webhook_id,
            "timestamp": event_data.get("timestamp"),
        }

        signature = generate_hmac_signature(payload, secret)

        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": signature,
            "User-Agent": "ChainHook-Webhook/1.0",
        }

        if custom_headers:
            headers.update(custom_headers)

        for attempt in range(1, self.max_retries + 1):
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        url, json=payload, headers=headers, timeout=self.timeout
                    )

                    with get_db_session() as session:
                        if response.status_code in [200, 201, 202, 204]:
                            create_webhook_log(
                                session,
                                webhook_id=webhook_id,
                                event_data=event_data,
                                status="success",
                                attempt=attempt,
                                response_code=response.status_code,
                                response_body=response.text[:1000],
                            )
                            logger.info(
                                f"✅ Webhook delivered successfully to {url} "
                                f"(attempt {attempt}): {response.status_code}"
                            )
                            return True
                        else:
                            create_webhook_log(
                                session,
                                webhook_id=webhook_id,
                                event_data=event_data,
                                status="retrying"
                                if attempt < self.max_retries
                                else "failed",
                                attempt=attempt,
                                response_code=response.status_code,
                                response_body=response.text[:1000],
                                error_message=f"HTTP {response.status_code}",
                            )
                            logger.warning(
                                f"⚠️ Webhook delivery failed to {url} "
                                f"(attempt {attempt}): {response.status_code}"
                            )

            except httpx.TimeoutException as e:
                with get_db_session() as session:
                    create_webhook_log(
                        session,
                        webhook_id=webhook_id,
                        event_data=event_data,
                        status="retrying" if attempt < self.max_retries else "failed",
                        attempt=attempt,
                        error_message=f"Timeout: {str(e)}",
                    )
                logger.error(f"⏱️ Webhook timeout to {url} (attempt {attempt})")

            except Exception as e:
                with get_db_session() as session:
                    create_webhook_log(
                        session,
                        webhook_id=webhook_id,
                        event_data=event_data,
                        status="retrying" if attempt < self.max_retries else "failed",
                        attempt=attempt,
                        error_message=str(e),
                    )
                logger.error(
                    f"❌ Webhook delivery error to {url} (attempt {attempt}): {str(e)}"
                )

            if attempt < self.max_retries:
                wait_time = self.backoff_base**attempt
                logger.info(f"⏳ Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)

        logger.error(f"❌ Failed to deliver webhook after {self.max_retries} attempts")
        return False
