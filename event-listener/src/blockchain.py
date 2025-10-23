import asyncio
from web3 import Web3
from web3.providers.websocket import WebsocketProvider  # Correct import for v6+
from .config import settings
from .queue import publish_event

class EventListener:
    def __init__(self):
        # Use WebsocketProvider for web3.py v6+
        self.w3 = Web3(WebsocketProvider(settings.web3_provider_url))
        self.running = False
        # Sample ERC20 contract address on Sepolia (e.g., LINK token)
        self.contract_address = "0x779877A7B0D9E8603169DdbD7836e478b4624789"
        self.abi = [
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True, "name": "from", "type": "address"},
                    {"indexed": True, "name": "to", "type": "address"},
                    {"indexed": False, "name": "value", "type": "uint256"}
                ],
                "name": "Transfer",
                "type": "event"
            }
        ]

    async def start(self):
        if not self.w3.is_connected():
            raise Exception("Failed to connect to Web3 provider")
        self.running = True
        try:
            contract = self.w3.eth.contract(address=self.contract_address, abi=self.abi)
            # Create event filter for Transfer events
            event_filter = contract.events.Transfer.create_filter(fromBlock="latest")
            while self.running:
                try:
                    # Poll for new events
                    for event in event_filter.get_new_entries():
                        event_data = {
                            "contract_id": self.contract_address,
                            "event_type": "Transfer",
                            "data": {
                                "from": event["args"]["from"],
                                "to": event["args"]["to"],
                                "value": str(event["args"]["value"]),  # Convert to string for JSON serialization
                                "blockNumber": event["blockNumber"],
                                "transactionHash": event["transactionHash"].hex()
                            }
                        }
                        await publish_event(event_data)
                    await asyncio.sleep(2)  # Poll every 2 seconds
                except Exception as e:
                    print(f"Error in event polling: {e}")
                    await asyncio.sleep(5)  # Wait before retrying
        except Exception as e:
            print(f"Error in listener setup: {e}")
            self.running = False
            raise

    async def stop(self):
        self.running = False