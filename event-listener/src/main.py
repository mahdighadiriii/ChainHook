import asyncio
import logging
from typing import List

from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from .bitcoin_listener import get_bitcoin_listener, start_bitcoin_listener, track_btc
from .blockchain import EventListener
from .cache import cache_events, get_cached_events
from .database import get_db_session, get_events, save_contract
from .models import ContractCreate, EventResponse
from .rabbitmq_client import get_publisher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ChainHook Event Listener")


@app.on_event("startup")
async def startup():
    logger.info("🚀 Starting ChainHook Event Listener...")

    # Initialize RabbitMQ
    try:
        publisher = get_publisher()
        logger.info("✅ RabbitMQ publisher initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize RabbitMQ: {e}")

    # Start Ethereum listener
    app.state.listener = EventListener()
    app.state.listener_task = asyncio.create_task(app.state.listener.start())
    logger.info("✅ Ethereum/Web3 listener started")

    # Load existing Bitcoin addresses and start listener
    with get_db_session() as session:
        result = session.execute(
            text("SELECT address FROM event_listener.contracts WHERE coin='bitcoin'")
        ).fetchall()

        bitcoin_listener = get_bitcoin_listener()
        for row in result:
            address = row[0]
            bitcoin_listener.add_address(address)
            logger.info(f"✅ Loaded Bitcoin address for tracking: {address}")

    # Start Bitcoin listener task
    app.state.bitcoin_listener_task = asyncio.create_task(start_bitcoin_listener())
    logger.info("✅ Bitcoin listener started")


@app.on_event("shutdown")
async def shutdown():
    logger.info("🛑 Shutting down...")

    # Stop Ethereum listener
    if hasattr(app.state, "listener"):
        await app.state.listener.stop()

    if hasattr(app.state, "listener_task"):
        app.state.listener_task.cancel()
        try:
            await app.state.listener_task
        except asyncio.CancelledError:
            pass

    # Stop Bitcoin listener
    if hasattr(app.state, "bitcoin_listener_task"):
        app.state.bitcoin_listener_task.cancel()
        try:
            await app.state.bitcoin_listener_task
        except asyncio.CancelledError:
            pass

    # Close RabbitMQ
    publisher = get_publisher()
    if publisher:
        publisher.close()

    logger.info("✅ Shutdown complete")


@app.post("/contracts/register", response_model=ContractCreate)
async def register_contract(contract: ContractCreate):
    """Register a new contract/address to track"""
    with get_db_session() as session:
        saved_contract = save_contract(
            session, contract.address, contract.abi, contract.coin
        )

        if contract.coin == "bitcoin":
            # Add to Bitcoin listener
            await track_btc(contract.address)
            logger.info(f"✅ Started Bitcoin monitoring for {contract.address}")

        elif contract.coin == "ethereum":
            # Reload Ethereum contracts
            if hasattr(app.state, "listener"):
                app.state.listener.load_contracts()
                logger.info(f"✅ Reloaded Ethereum contracts, added {contract.address}")

    return contract


@app.get("/contracts/{contract_id}/events", response_model=List[EventResponse])
async def get_contract_events(contract_id: str):
    """Get events for a specific contract"""
    # Check cache first
    cached = get_cached_events(contract_id)
    if cached:
        logger.info(f"📦 Returning cached events for {contract_id}")
        return cached

    # Fetch from database
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

        # Cache the results
        cache_events(contract_id, response_events)
        return response_events


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "ethereum_listener": hasattr(app.state, "listener"),
        "bitcoin_listener": hasattr(app.state, "bitcoin_listener_task"),
    }


@app.get("/status")
async def status():
    """Get status of all listeners"""
    bitcoin_listener = get_bitcoin_listener()

    return {
        "ethereum": {
            "active": hasattr(app.state, "listener"),
            "contracts": len(app.state.listener.contracts)
            if hasattr(app.state, "listener")
            else 0,
        },
        "bitcoin": {
            "active": hasattr(app.state, "bitcoin_listener_task"),
            "addresses": list(bitcoin_listener.tracked_addresses),
            "count": len(bitcoin_listener.tracked_addresses),
        },
    }
