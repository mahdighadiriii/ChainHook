import time
import uuid
from datetime import datetime

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import JSON, Column, DateTime, String, create_engine, text
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


def parse_postgres_url(url):
    from urllib.parse import urlparse

    parsed = urlparse(url)
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "user": parsed.username,
        "password": parsed.password,
        "database": parsed.path.lstrip("/"),
    }


def connect_with_retry(db_config, retries=5, delay=2):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(
                dbname="postgres",
                user=db_config["user"],
                password=db_config["password"],
                host=db_config["host"],
                port=db_config["port"],
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            return conn
        except psycopg2.OperationalError as e:
            if attempt < retries - 1:
                print(f"Connection failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise


def create_database_if_not_exists(db_config):
    """Create database if it doesn't exist (PostgreSQL compatible)"""
    conn = connect_with_retry(db_config)
    cursor = conn.cursor()

    # Check if database exists
    cursor.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s", (db_config["database"],)
    )
    exists = cursor.fetchone()

    if not exists:
        # Create database
        cursor.execute(f"CREATE DATABASE {db_config['database']}")
        print(f"Database {db_config['database']} created successfully")
    else:
        print(f"Database {db_config['database']} already exists")

    cursor.close()
    conn.close()


# Create database if it doesn't exist
db_config = parse_postgres_url(settings.postgres_url)
create_database_if_not_exists(db_config)

# Create engine and tables
engine = create_engine(settings.postgres_url, echo=True)
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS event_listener"))
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


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
