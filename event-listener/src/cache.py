import json

import redis

from .config import settings
from .models import EventResponse

client = redis.Redis.from_url(settings.redis_url)


def cache_events(contract_id: str, events: list):
    client.setex(
        f"events:{contract_id}", 3600, json.dumps([e.__dict__ for e in events])
    )


def get_cached_events(contract_id: str):
    data = client.get(f"events:{contract_id}")
    if data:
        return [EventResponse(**e) for e in json.loads(data)]
    return None
