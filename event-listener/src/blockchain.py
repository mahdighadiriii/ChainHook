import asyncio
import json
import logging

from sqlalchemy import create_engine, text
from web3 import Web3
from web3.providers.websocket import WebsocketProvider

from .config import settings
from .rabbitmq_client import publish_event

logger = logging.getLogger(__name__)


class EventListener:
    def __init__(self):
        self.w3 = Web3(WebsocketProvider(settings.web3_provider_url))
        self.engine = create_engine(settings.postgres_url)
        self.running = False
        self.contracts = []

    def load_contracts(self):
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT address, abi FROM event_listener.contracts")
            )
            self.contracts = [
                {
                    "address": row[0],
                    "contract": self.w3.eth.contract(
                        address=Web3.to_checksum_address(row[0]), abi=json.loads(row[1])
                    ),
                    "contract_id": row[0].lower(),
                }
                for row in result
            ]
        if not self.contracts:
            logger.warning("No contracts registered in the database.")

    async def start(self):
        if not self.w3.is_connected():
            raise Exception("Failed to connect to Web3 provider")

        self.running = True
        logger.info("Starting blockchain event listener")

        try:
            self.load_contracts()
            if not self.contracts:
                logger.info("No contracts to monitor. Waiting for API registration...")
                while self.running and not self.contracts:
                    await asyncio.sleep(10)
                    self.load_contracts()
                if not self.running:
                    return

            for contract_info in self.contracts:
                logger.info(f"Listening to contract at {contract_info['address']}")

            while self.running:
                try:
                    for contract_info in self.contracts:
                        contract = contract_info["contract"]
                        contract_id = contract_info["contract_id"]
                        event_filter = contract.events.Transfer.create_filter(
                            fromBlock="latest"
                        )
                        for event in event_filter.get_new_entries():
                            event_data = {
                                "contract_id": contract_id,
                                "event_type": "Transfer",
                                "data": {
                                    "from": event["args"]["from"],
                                    "to": event["args"]["to"],
                                    "value": str(event["args"]["value"]),
                                    "blockNumber": event["blockNumber"],
                                    "transactionHash": event["transactionHash"].hex(),
                                },
                            }
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
