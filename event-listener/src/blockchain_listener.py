import asyncio
import json
import logging

from sqlalchemy import create_engine, text
from web3 import Web3

from src.config import settings
from src.rabbitmq_client import publish_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Web3Listener:
    def __init__(self):
        self.w3 = Web3(Web3.WebsocketProvider(settings.web3_provider_url))
        self.engine = create_engine(settings.postgres_url)
        self.contracts = []
        self.last_block = 0

    def setup_contracts(self):
        if not self.w3.is_connected():
            logger.error("WebSocket connection failed!")
            raise ConnectionError("Cannot connect to WebSocket")

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
                    "contract_id": f"contract-{row[0].lower()}",
                }
                for row in result if row[2] == 'ethereum'
            ]

        if not self.contracts:
            logger.warning("No contracts registered in the database.")
            return

        self.last_block = self.w3.eth.block_number
        for contract_info in self.contracts:
            logger.info(
                f"✅ Listening to contract at {contract_info['address']} (ID: {contract_info['contract_id']})"
            )
        logger.info(f"✅ Starting from block: {self.last_block}")

    async def poll_events(self, from_block: int, to_block: int):
        try:
            logger.info(f"🔍 Polling blocks {from_block} to {to_block}")
            for contract_info in self.contracts:
                contract = contract_info["contract"]
                contract_id = contract_info["contract_id"]
                logs = contract.events.Transfer().get_logs(
                    fromBlock=from_block, toBlock=to_block
                )

                if logs:
                    logger.info(
                        f"🎉 Found {len(logs)} Transfer events for {contract_id}!"
                    )

                for log in logs:
                    value = log["args"]["value"]
                    human_value = value / 1_000_000

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
                        f"💸 Transfer: {log['args']['from'][:8]}... → {log['args']['to'][:8]}... | {human_value} | Block: {log['blockNumber']} | Contract: {contract_id}"
                    )
        except Exception as e:
            logger.error(f"❌ Failed to fetch logs: {e}", exc_info=True)

    async def start(self):
        logger.info("🚀 Web3 Listener STARTED — watching registered contracts")
        self.setup_contracts()

        while True:
            try:
                if not self.contracts:
                    logger.info(
                        "No contracts to monitor. Waiting for API registration..."
                    )
                    await asyncio.sleep(10)
                    self.setup_contracts()
                    continue

                latest = self.w3.eth.block_number
                if latest > self.last_block:
                    from_block = max(self.last_block + 1, latest - 4)
                    await self.poll_events(from_block, latest)
                    self.last_block = latest
                else:
                    logger.debug(f"No new blocks. Current: {latest}")

                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"❌ Web3 error: {e}", exc_info=True)
                await asyncio.sleep(5)


async def main():
    listener = Web3Listener()
    await listener.start()


if __name__ == "__main__":
    asyncio.run(main())
