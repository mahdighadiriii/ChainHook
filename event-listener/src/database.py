import uuid
from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from .config import settings

Base = declarative_base()


class Contract(Base):
    __tablename__ = "contracts"
    __table_args__ = {"schema": "event_listener"}
    address = Column(String, primary_key=True)
    abi = Column(JSON)


class Event(Base):
    __tablename__ = "events"
    __table_args__ = {"schema": "event_listener"}
    id = Column(String, primary_key=True)
    contract_id = Column(String)
    event_type = Column(String)
    data = Column(JSON)
    timestamp = Column(DateTime)


engine = create_engine(settings.postgres_url)
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)


def get_db_session():
    return SessionLocal()


def save_contract(session, address: str, abi: dict):
    contract = Contract(address=address, abi=abi)
    session.merge(contract)
    session.commit()


def save_event(session, contract_id: str, event_type: str, data: dict):
    event = Event(
        id=str(uuid.uuid4()),
        contract_id=contract_id,
        event_type=event_type,
        data=data,
        timestamp=datetime.utcnow(),
    )
    session.add(event)
    session.commit()


def get_events(session, contract_id: str):
    return session.query(Event).filter(Event.contract_id == contract_id).all()
