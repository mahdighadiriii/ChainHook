from typing import Dict, Optional

from pydantic import BaseModel, HttpUrl


class WebhookCreate(BaseModel):
    url: HttpUrl
    event_types: list[str]
    contract_id: Optional[str] = None
    description: Optional[str] = None
    headers: Optional[Dict[str, str]] = None


class WebhookResponse(BaseModel):
    id: str
    url: str
    event_types: list[str]
    contract_id: Optional[str]
    description: Optional[str]
    is_active: bool
    created_at: str
    headers: Optional[Dict[str, str]] = None


class WebhookLogResponse(BaseModel):
    id: str
    webhook_id: str
    event_data: Dict
    status: str
    attempt: int
    response_code: Optional[int]
    response_body: Optional[str]
    error_message: Optional[str]
    timestamp: str
