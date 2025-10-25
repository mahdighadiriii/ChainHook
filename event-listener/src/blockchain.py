import asyncio
import json
import logging

from sqlalchemy import text
from web3 import Web3

from src.config import settings
from src.database import get_db_session
from src.rabbitmq_client import publish_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EventListener:
    def __init__(self):
        self.w3 = None
        self.contracts = []
        self.last_block = 0
        self.running = False

    def connect_web3(self):
        """Connect to Web3 provider"""
        if not settings.web3_provider_url:
            logger.warning("No Web3 provider URL configured")
            return False

        try:
            self.w3 = Web3(Web3.WebsocketProvider(settings.web3_provider_url))
            if self.w3.is_connected():
                logger.info("✅ Connected to Ethereum Web3 provider")
                return True
            else:
                logger.error("❌ Failed to connect to Web3 provider")
                return False
        except Exception as e:
            logger.error(f"❌ Error connecting to Web3: {e}")
            return False

    def load_contracts(self):
        """Load Ethereum contracts from database"""
        if not self.w3 or not self.w3.is_connected():
            if not self.connect_web3():
                return

        with get_db_session() as session:
            result = session.execute(
                text(
                    "SELECT address, abi FROM event_listener.contracts WHERE coin='ethereum'"
                )
            )

            self.contracts = []
            for row in result:
                address = row[0]
                abi = json.loads(row[1]) if row[1] else []

                try:
                    contract = self.w3.eth.contract(
                        address=Web3.to_checksum_address(address), abi=abi
                    )
                    self.contracts.append(
                        {
                            "address": address,
                            "contract": contract,
                            "contract_id": f"ethereum-{address.lower()}",
                        }
                    )
                    logger.info(f"✅ Loaded Ethereum contract: {address}")
                except Exception as e:
                    logger.error(f"❌ Error loading contract {address}: {e}")

        if not self.contracts:
            logger.warning("No Ethereum contracts registered in the database.")
        else:
            logger.info(f"Loaded {len(self.contracts)} Ethereum contract(s)")

    async def poll_events(self, from_block: int, to_block: int):
        """Poll for events in a block range"""
        try:
            logger.debug(f"🔍 Polling blocks {from_block} to {to_block}")

            for contract_info in self.contracts:
                contract = contract_info["contract"]
                contract_id = contract_info["contract_id"]

                try:
                    logs = contract.events.Transfer().get_logs(
                        fromBlock=from_block, toBlock=to_block
                    )

                    if logs:
                        logger.info(
                            f"🎉 Found {len(logs)} Transfer events for {contract_id}!"
                        )

                    for log in logs:
                        value = log["args"]["value"]
                        human_value = (
                            value / 1_000_000
                        )

                        event = {
                            "contract_id": contract_id,
                            "event_type": "Transfer",
                            "data": {
                                "from": log["args"]["from"],
                                "to": log["args"]["to"],
                                "value": str(value),
                                "value_human": human_value,
                                "tx_hash": log["transactionHash"].hex(),
                                "block": log["blockNumber"],
                            },
                        }

                        await publish_event(event)

                        logger.info(
                            f"💸 Transfer: {log['args']['from'][:8]}... → {log['args']['to'][:8]}... | "
                            f"{human_value} | Block: {log['blockNumber']} | Contract: {contract_id}"
                        )
                except Exception as e:
                    logger.debug(f"No Transfer events or error: {e}")

        except Exception as e:
            logger.error(f"❌ Failed to poll events: {e}", exc_info=True)

    async def start(self):
        """Start the Ethereum listener"""
        logger.info("Starting blockchain event listener")

        if not self.connect_web3():
            logger.error("Cannot start Ethereum listener without Web3 connection")
            return

        self.load_contracts()

        if not self.contracts:
            logger.info(
                "No Ethereum contracts to monitor. Skipping Ethereum listener..."
            )
            return

        self.running = True
        self.last_block = self.w3.eth.block_number
        logger.info(f"✅ Starting from block: {self.last_block}")
        logger.info("🚀 Ethereum Listener STARTED — watching registered contracts")

        while self.running:
            try:
                if len(self.contracts) == 0:
                    logger.debug("No contracts to monitor. Checking database...")
                    self.load_contracts()
                    if not self.contracts:
                        await asyncio.sleep(30)
                        continue

                latest = self.w3.eth.block_number

                if latest > self.last_block:
                    from_block = self.last_block + 1
                    to_block = min(
                        latest, from_block + 99
                    )

                    await self.poll_events(from_block, to_block)
                    self.last_block = to_block
                else:
                    logger.debug(f"No new blocks. Current: {latest}")

                await asyncio.sleep(2)

            except Exception as e:
                logger.error(f"❌ Ethereum listener error: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def stop(self):
        """Stop the listener"""
        logger.info("Stopping Ethereum listener...")
        self.running = False
