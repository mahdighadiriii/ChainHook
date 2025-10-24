import asyncio
import logging
from typing import List

from fastapi import FastAPI

from .blockchain import EventListener
from .cache import cache_events, get_cached_events
from .database import get_db_session, get_events, save_contract
from .models import ContractCreate, EventResponse
from .rabbitmq_client import get_publisher

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ChainHook Event Listener")


@app.on_event("startup")
async def startup():
    # Initialize RabbitMQ publisher
    try:
        publisher = get_publisher()
        logger.info("RabbitMQ publisher initialized")
    except Exception as e:
        logger.error(f"Failed to initialize RabbitMQ: {e}")

    app.state.listener = EventListener()
    app.state.listener_task = asyncio.create_task(app.state.listener.start())
    logger.info("Blockchain listener started")


@app.on_event("shutdown")
async def shutdown():
    await app.state.listener.stop()
    if hasattr(app.state, "listener_task"):
        app.state.listener_task.cancel()
        try:
            await app.state.listener_task
        except asyncio.CancelledError:
            pass

    from .queue import publisher

    if publisher:
        publisher.close()

    logger.info("Shutdown complete")


@app.post("/contracts/register", response_model=ContractCreate)
async def register_contract(contract: ContractCreate):
    with get_db_session() as session:
        save_contract(session, contract.address, contract.abi)
    return contract


@app.get("/contracts/{contract_id}/events", response_model=List[EventResponse])
async def get_contract_events(contract_id: str):
    cached = get_cached_events(contract_id)
    if cached:
        return cached

    with get_db_session() as session:
        events = get_events(session, contract_id)
        response_events = [
            EventResponse(
                id=e.id,
                contract_id=e.contract_id,
                event_type=e.event_type,
                data=e.data,
                timestamp=e.timestamp.isoformat(),
            )
            for e in events
        ]
        cache_events(contract_id, response_events)
        return response_events


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
