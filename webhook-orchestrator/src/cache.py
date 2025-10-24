import json
from typing import List, Optional

import redis

from .config import settings

client = redis.Redis.from_url(settings.redis_url)


def cache_webhooks(webhooks: List[dict]):
    """Cache all active webhooks"""
    client.setex("active_webhooks", 3600, json.dumps(webhooks))


def get_cached_webhooks() -> Optional[List[dict]]:
    """Get cached webhooks"""
    data = client.get("active_webhooks")
    if data:
        return json.loads(data)
    return None


def invalidate_webhook_cache():
    """Invalidate webhook cache"""
    client.delete("active_webhooks")
