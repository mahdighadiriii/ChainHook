from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel
from sqlalchemy import Column, DateTime, Integer, JSON, String, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Contract(Base):
    __tablename__ = "contracts"
    __table_args__ = {"schema": "event_listener"}

    id = Column(Integer, primary_key=True)
    address = Column(String(255), unique=True, nullable=False)
    abi = Column(JSON, nullable=True)
    coin = Column(String(50), nullable=False, default="ethereum")
    created_at = Column(DateTime, default=datetime.utcnow)


class Event(Base):
    __tablename__ = "events"
    __table_args__ = {"schema": "event_listener"}

    id = Column(Integer, primary_key=True)
    contract_id = Column(String(255), nullable=False)
    event_type = Column(String(100), nullable=False)
    data = Column(JSON, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)


# Pydantic models
class ContractCreate(BaseModel):
    address: str
    abi: Optional[Any] = None
    coin: str = "ethereum"

class EventResponse(BaseModel):
    id: int
    contract_id: str
    event_type: str
    data: Dict[str, Any]
    timestamp: str