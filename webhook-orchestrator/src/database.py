import time
import uuid
from datetime import datetime
from typing import Optional

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    create_engine,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from .config import settings

Base = declarative_base()


class Webhook(Base):
    __tablename__ = "webhooks"
    __table_args__ = {"schema": "webhook_orchestrator"}

    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    event_types = Column(JSON, nullable=False)
    contract_id = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    secret = Column(String, nullable=False)
    headers = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class WebhookLog(Base):
    __tablename__ = "webhook_logs"
    __table_args__ = {"schema": "webhook_orchestrator"}

    id = Column(String, primary_key=True)
    webhook_id = Column(String, nullable=False)
    event_data = Column(JSON, nullable=False)
    status = Column(String, nullable=False)
    attempt = Column(Integer, default=1)
    response_code = Column(Integer, nullable=True)
    response_body = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)


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
    """Create database if it doesn't exist"""
    conn = connect_with_retry(db_config)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s", (db_config["database"],)
    )
    exists = cursor.fetchone()

    if not exists:
        cursor.execute(f"CREATE DATABASE {db_config['database']}")
        print(f"Database {db_config['database']} created successfully")
    else:
        print(f"Database {db_config['database']} already exists")

    cursor.close()
    conn.close()


# Initialize database
db_config = parse_postgres_url(settings.postgres_url)
create_database_if_not_exists(db_config)

engine = create_engine(settings.postgres_url, echo=True)
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS webhook_orchestrator"))
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db_session():
    return SessionLocal()


def create_webhook(
    session,
    url: str,
    event_types: list,
    contract_id: Optional[str],
    description: Optional[str],
    secret: str,
    headers: Optional[dict],
):
    webhook = Webhook(
        id=str(uuid.uuid4()),
        url=url,
        event_types=event_types,
        contract_id=contract_id,
        description=description,
        secret=secret,
        headers=headers,
        created_at=datetime.utcnow(),
    )
    session.add(webhook)
    session.commit()
    return webhook


def get_webhook(session, webhook_id: str):
    return session.query(Webhook).filter(Webhook.id == webhook_id).first()


def get_all_webhooks(session, active_only: bool = True):
    query = session.query(Webhook)
    if active_only:
        query = query.filter(Webhook.is_active == True)
    return query.all()


def delete_webhook(session, webhook_id: str):
    webhook = session.query(Webhook).filter(Webhook.id == webhook_id).first()
    if webhook:
        session.delete(webhook)
        session.commit()
        return True
    return False


def create_webhook_log(
    session,
    webhook_id: str,
    event_data: dict,
    status: str,
    attempt: int = 1,
    response_code: Optional[int] = None,
    response_body: Optional[str] = None,
    error_message: Optional[str] = None,
):
    log = WebhookLog(
        id=str(uuid.uuid4()),
        webhook_id=webhook_id,
        event_data=event_data,
        status=status,
        attempt=attempt,
        response_code=response_code,
        response_body=response_body,
        error_message=error_message,
        timestamp=datetime.utcnow(),
    )
    session.add(log)
    session.commit()
    return log


def get_webhook_logs(session, webhook_id: str, limit: int = 100):
    return (
        session.query(WebhookLog)
        .filter(WebhookLog.webhook_id == webhook_id)
        .order_by(WebhookLog.timestamp.desc())
        .limit(limit)
        .all()
    )
