import asyncio
import logging

from web3 import Web3
from web3.providers.websocket import WebsocketProvider

from .config import settings
from .queue import publish_event

logger = logging.getLogger(__name__)


class EventListener:
    def __init__(self):
        self.w3 = Web3(WebsocketProvider(settings.web3_provider_url))
        self.running = False
        self.contract_address = "0x779877A7B0D9E8603169DdbD7836e478b4624789"
        self.abi = [
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True, "name": "from", "type": "address"},
                    {"indexed": True, "name": "to", "type": "address"},
                    {"indexed": False, "name": "value", "type": "uint256"},
                ],
                "name": "Transfer",
                "type": "event",
            }
        ]

    async def start(self):
        if not self.w3.is_connected():
            raise Exception("Failed to connect to Web3 provider")

        self.running = True
        logger.info("Starting blockchain event listener")

        try:
            contract = self.w3.eth.contract(address=self.contract_address, abi=self.abi)
            event_filter = contract.events.Transfer.create_filter(fromBlock="latest")

            while self.running:
                try:
                    for event in event_filter.get_new_entries():
                        event_data = {
                            "contract_id": self.contract_address,
                            "event_type": "Transfer",
                            "data": {
                                "from": event["args"]["from"],
                                "to": event["args"]["to"],
                                "value": str(event["args"]["value"]),
                                "blockNumber": event["blockNumber"],
                                "transactionHash": event["transactionHash"].hex(),
                            },
                        }

                        # Publish to RabbitMQ
                        await publish_event(event_data)
                        logger.info(
                            f"Event published: {event['transactionHash'].hex()}"
                        )

                    await asyncio.sleep(2)

                except Exception as e:
                    logger.error(f"Error in event polling: {e}")
                    await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"Error in listener setup: {e}")
            self.running = False
            raise

    async def stop(self):
        self.running = False
        logger.info("Stopping blockchain event listener")
