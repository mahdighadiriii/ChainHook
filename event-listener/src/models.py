from typing import Dict, List

from pydantic import BaseModel


class ContractCreate(BaseModel):
    address: str
    abi: List[Dict]


class EventResponse(BaseModel):
    id: str
    contract_id: str
    event_type: str
    data: Dict
    timestamp: str
