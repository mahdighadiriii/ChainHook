import asyncio
import logging
from typing import List

from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from .bitcoin_listener import track_btc
from .blockchain import EventListener
from .cache import cache_events, get_cached_events
from .database import get_db_session, get_events, save_contract  # No init_db
from .models import ContractCreate, EventResponse
from .rabbitmq_client import get_publisher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ChainHook Event Listener")

@app.on_event("startup")
async def startup():
    logger.info("Database initialization handled on import")
    try:
        publisher = get_publisher()
        logger.info("RabbitMQ publisher initialized")
    except Exception as e:
        logger.error(f"Failed to initialize RabbitMQ: {e}")
    app.state.listener = EventListener()
    app.state.listener_task = asyncio.create_task(app.state.listener.start())
    logger.info("Ethereum/Web3 listener started")
    btc_tasks = []
    with get_db_session() as session:
        result = session.execute(
            text("SELECT address FROM event_listener.contracts WHERE coin='bitcoin'")
        ).fetchall()
        for row in result:
            address = row[0]
            task = asyncio.create_task(track_btc(address))
            btc_tasks.append(task)
            logger.info(f"Bitcoin listener started for address {address}")
    app.state.btc_tasks = btc_tasks

@app.on_event("shutdown")
async def shutdown():
    await app.state.listener.stop()
    if hasattr(app.state, "listener_task"):
        app.state.listener_task.cancel()
        try:
            await app.state.listener_task
        except asyncio.CancelledError:
            pass
    if hasattr(app.state, "btc_tasks"):
        for task in app.state.btc_tasks:
            task.cancel()
        await asyncio.gather(*app.state.btc_tasks, return_exceptions=True)
    publisher = get_publisher()
    if publisher:
        publisher.close()
    logger.info("Shutdown complete")

@app.post("/contracts/register", response_model=ContractCreate)
async def register_contract(contract: ContractCreate):
    with get_db_session() as session:
        saved_contract = save_contract(
            session, contract.address, contract.abi, contract.coin
        )
        if contract.coin == "bitcoin":
            task = asyncio.create_task(track_btc(contract.address))
            if not hasattr(app.state, "btc_tasks"):
                app.state.btc_tasks = []
            app.state.btc_tasks.append(task)
            logger.info(f"Started Bitcoin monitoring for {contract.address}")
        elif contract.coin == "ethereum":
            if hasattr(app.state, "listener"):
                app.state.listener.load_contracts()
                logger.info(f"Reloaded Ethereum contracts, added {contract.address}")
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