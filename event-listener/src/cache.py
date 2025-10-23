import json
from typing import List

import redis

from .config import settings
from .models import EventResponse

client = redis.Redis.from_url(settings.redis_url)


def cache_events(contract_id: str, events: List[EventResponse]):
    """Cache list of EventResponse objects"""
    client.setex(
        f"events:{contract_id}", 3600, json.dumps([e.model_dump() for e in events])
    )


def get_cached_events(contract_id: str) -> List[EventResponse] | None:
    data = client.get(f"events:{contract_id}")
    if data:
        return [EventResponse(**e) for e in json.loads(data)]
    return None
