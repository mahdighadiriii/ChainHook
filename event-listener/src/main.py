import asyncio
from typing import List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .blockchain import EventListener
from .cache import cache_events, get_cached_events
from .config import settings
from .database import get_db_session, get_events, save_contract, save_event
from .models import ContractCreate, EventResponse
from .queue import publish_event

app = FastAPI(title="ChainHook Event Listener")


@app.on_event("startup")
async def startup():
    app.state.listener = EventListener()
    # Run listener in background task instead of blocking
    app.state.listener_task = asyncio.create_task(app.state.listener.start())


@app.on_event("shutdown")
async def shutdown():
    await app.state.listener.stop()
    # Wait for listener task to finish
    if hasattr(app.state, "listener_task"):
        app.state.listener_task.cancel()
        try:
            await app.state.listener_task
        except asyncio.CancelledError:
            pass


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
        cache_events(contract_id, events)
        return events


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
