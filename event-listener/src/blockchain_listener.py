import asyncio
import logging

from web3 import Web3

from src.config import settings
from src.rabbitmq_client import (
    publish_event,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# USDC Sepolia
USDC_SEPOLIA_ADDRESS = "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238"
USDC_SEPOLIA_ID = "usdc-sepolia"

USDC_ABI = [
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


class Web3Listener:
    def __init__(self):
        self.w3 = Web3(Web3.WebsocketProvider(settings.web3_provider_url))
        self.contract = None
        self.last_block = 0

    def setup_contract(self):
        if not self.w3.is_connected():
            logger.error("WebSocket connection failed!")
            raise ConnectionError("Cannot connect to WebSocket")

        self.contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(USDC_SEPOLIA_ADDRESS), abi=USDC_ABI
        )
        self.last_block = self.w3.eth.block_number
        logger.info(f"✅ Listening to USDC (Sepolia) at {USDC_SEPOLIA_ADDRESS}")
        logger.info(f"✅ Starting from block: {self.last_block}")

    async def poll_events(self, from_block: int, to_block: int):
        try:
            logger.info(f"🔍 Polling blocks {from_block} to {to_block}")
            logs = self.contract.events.Transfer().get_logs(
                fromBlock=from_block, toBlock=to_block
            )

            if logs:
                logger.info(f"🎉 Found {len(logs)} USDC Transfer events!")

            for log in logs:
                value = log["args"]["value"]
                human_value = value / 1_000_000

                event = {
                    "contract_id": USDC_SEPOLIA_ID,
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
                    f"💸 USDC Transfer: {log['args']['from'][:8]}... → {log['args']['to'][:8]}... | {human_value} USDC | Block: {log['blockNumber']}"
                )
        except Exception as e:
            logger.error(f"❌ Failed to fetch logs: {e}", exc_info=True)

    async def start(self):
        logger.info("🚀 Web3 Listener STARTED — watching USDC on Sepolia")
        self.setup_contract()

        while True:
            try:
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
