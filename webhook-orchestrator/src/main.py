import asyncio
import logging
import secrets
from typing import List

from fastapi import FastAPI, HTTPException

from .cache import invalidate_webhook_cache
from .consumer import WebhookConsumer
from .database import (
    create_webhook,
    delete_webhook,
    get_db_session,
    get_webhook,
    get_webhook_logs,
)
from .models import WebhookCreate, WebhookLogResponse, WebhookResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ChainHook Webhook Orchestrator")


@app.on_event("startup")
async def startup():
    """Start the webhook consumer on startup"""
    app.state.consumer = WebhookConsumer()
    app.state.consumer_task = asyncio.create_task(app.state.consumer.start())
    logger.info("🚀 Webhook Orchestrator started")


@app.on_event("shutdown")
async def shutdown():
    """Graceful shutdown"""
    if hasattr(app.state, "consumer_task"):
        app.state.consumer_task.cancel()
        try:
            await app.state.consumer_task
        except asyncio.CancelledError:
            pass
    logger.info("🛑 Webhook Orchestrator stopped")


@app.post("/webhooks/create", response_model=WebhookResponse)
async def create_webhook_endpoint(webhook: WebhookCreate):
    """Register a new webhook"""
    try:
        secret = secrets.token_urlsafe(32)

        with get_db_session() as session:
            db_webhook = create_webhook(
                session,
                url=str(webhook.url),
                event_types=webhook.event_types,
                contract_id=webhook.contract_id,
                description=webhook.description,
                secret=secret,
                headers=webhook.headers,
            )

            invalidate_webhook_cache()

            return WebhookResponse(
                id=db_webhook.id,
                url=db_webhook.url,
                event_types=db_webhook.event_types,
                contract_id=db_webhook.contract_id,
                description=db_webhook.description,
                is_active=db_webhook.is_active,
                created_at=db_webhook.created_at.isoformat(),
                headers=db_webhook.headers,
            )
    except Exception as e:
        logger.error(f"Failed to create webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/webhooks/{webhook_id}", response_model=WebhookResponse)
async def get_webhook_endpoint(webhook_id: str):
    """Get webhook details"""
    with get_db_session() as session:
        webhook = get_webhook(session, webhook_id)
        if not webhook:
            raise HTTPException(status_code=404, detail="Webhook not found")

        return WebhookResponse(
            id=webhook.id,
            url=webhook.url,
            event_types=webhook.event_types,
            contract_id=webhook.contract_id,
            description=webhook.description,
            is_active=webhook.is_active,
            created_at=webhook.created_at.isoformat(),
            headers=webhook.headers,
        )


@app.delete("/webhooks/{webhook_id}")
async def delete_webhook_endpoint(webhook_id: str):
    """Delete a webhook"""
    with get_db_session() as session:
        success = delete_webhook(session, webhook_id)
        if not success:
            raise HTTPException(status_code=404, detail="Webhook not found")

        invalidate_webhook_cache()

        return {"message": "Webhook deleted successfully", "id": webhook_id}


@app.get("/webhooks/{webhook_id}/logs", response_model=List[WebhookLogResponse])
async def get_webhook_logs_endpoint(webhook_id: str, limit: int = 100):
    """Get delivery logs for a webhook"""
    with get_db_session() as session:
        webhook = get_webhook(session, webhook_id)
        if not webhook:
            raise HTTPException(status_code=404, detail="Webhook not found")

        logs = get_webhook_logs(session, webhook_id, limit)
        return [
            WebhookLogResponse(
                id=log.id,
                webhook_id=log.webhook_id,
                event_data=log.event_data,
                status=log.status,
                attempt=log.attempt,
                response_code=log.response_code,
                response_body=log.response_body,
                error_message=log.error_message,
                timestamp=log.timestamp.isoformat(),
            )
            for log in logs
        ]


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "webhook-orchestrator"}
