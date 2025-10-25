from solana.rpc.websocket_api import connect

from src.config import settings
from src.rabbitmq_client import publish_event


async def track_solana():
    async with connect(settings.solana_ws_url) as websocket:
        await websocket.logs_subscribe()
        async for response in websocket:
            log = response.result.value
            event = {"type": "Solana Log", "data": log}
            await publish_event(event)


if __name__ == "__main__":
    import asyncio

    asyncio.run(track_solana())
