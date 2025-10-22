import asyncio

from web3 import Web3

from .config import settings
from .queue import publish_event


class EventListener:
    def __init__(self):
        self.w3 = Web3(Web3.WebsocketProvider(settings.web3_provider_url))
        self.running = False

    async def start(self):
        self.running = True
        while self.running:
            try:
                contract_address = "0x..."
                abi = [...]
                contract = self.w3.eth.contract(address=contract_address, abi=abi)
                event_filter = contract.events.Transfer.create_filter(
                    fromBlock="latest"
                )
                for event in event_filter.get_new_entries():
                    event_data = {
                        "contract_id": contract_address,
                        "event_type": "Transfer",
                        "data": dict(event),
                    }
                    await publish_event(event_data)
                await asyncio.sleep(2)
            except Exception as e:
                print(f"Error in listener: {e}")
                await asyncio.sleep(5)

    async def stop(self):
        self.running = False
