import asyncio

import requests

from src.config import settings
from src.rabbitmq_client import publish_event


async def track_btc(address="1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"):
    while True:
        try:
            url = f"{settings.bitcoin_api_url}/addrs/{address}"
            response = requests.get(url)
            response_json = response.json()
            for tx in response_json.get("txs", [])[:5]:
                event = {"type": "BTC Transfer", "data": tx}
                await publish_event(event)
            await asyncio.sleep(30)
        except Exception as e:
            print(f"Error in Bitcoin monitoring: {e}")
            await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(track_btc())
