import asyncio
import logging
from datetime import datetime
from typing import Set

import aiohttp

from src.config import settings
from src.rabbitmq_client import publish_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BitcoinListener:
    def __init__(self):
        self.api_url = settings.bitcoin_api_url
        self.tracked_addresses: Set[str] = set()
        self.last_seen_txs: dict = {}  # address -> set of tx hashes
        logger.info(
            f"📡 Bitcoin API URL: {self.api_url[:60]}..."
            if self.api_url
            else "❌ No API URL configured!"
        )

    def add_address(self, address: str):
        """Add a new address to track"""
        if address not in self.tracked_addresses:
            self.tracked_addresses.add(address)
            self.last_seen_txs[address] = set()
            logger.info(f"🔍 Now tracking Bitcoin address: {address}")

    async def fetch_address_transactions(self, address: str):
        """Fetch transactions for a Bitcoin address"""
        try:
            url = f"{self.api_url}/addrs/{address}?limit=50"
            logger.info(f"🔍 Fetching transactions from BlockCypher...")

            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=15) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(
                            f"✅ Successfully fetched data from BlockCypher for {address}"
                        )

                        txrefs = data.get("txrefs", []) + data.get(
                            "unconfirmed_txrefs", []
                        )

                        if txrefs:
                            logger.info(
                                f"📦 Found {len(txrefs)} transaction references"
                            )
                            transactions = []
                            for txref in txrefs[:50]:
                                tx = {
                                    "hash": txref.get("tx_hash"),
                                    "block_height": txref.get("block_height"),
                                    "confirmations": txref.get("confirmations", 0),
                                    "confirmed": txref.get("confirmed"),
                                    "value": txref.get("value", 0),
                                    "tx_input_n": txref.get("tx_input_n", -1),
                                    "tx_output_n": txref.get("tx_output_n", -1),
                                }
                                transactions.append(tx)
                            return transactions
                        else:
                            logger.info(f"No transactions found for {address}")
                            return []

                    elif response.status == 429:
                        logger.warning(
                            f"⚠️  Rate limited (429). Waiting for next cycle..."
                        )
                        return []
                    else:
                        logger.error(f"❌ API returned status {response.status}")
                        response_text = await response.text()
                        logger.error(f"Response: {response_text[:200]}")
                        return []

        except asyncio.TimeoutError:
            logger.error(f"⏱️  Timeout fetching transactions")
            return []
        except Exception as e:
            logger.error(f"❌ Error: {e}", exc_info=True)
            return []

    async def fetch_from_blockchain_com(self, address: str, session):
        """Fallback to Blockchain.com API"""
        try:
            url = f"https://blockchain.info/rawaddr/{address}?limit=50"
            logger.info(f"Fetching from Blockchain.com: {address}")
            async with session.get(url, timeout=15) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(
                        f"✅ Successfully fetched data from Blockchain.com for {address}"
                    )
                    return self.convert_blockchain_com_format(data.get("txs", []))
                else:
                    logger.error(
                        f"Blockchain.com API returned status {response.status}"
                    )
                    response_text = await response.text()
                    logger.error(f"Response: {response_text[:200]}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching from Blockchain.com: {e}", exc_info=True)
            return []

    def convert_blockchain_com_format(self, txs):
        """Convert Blockchain.com transaction format to BlockCypher format"""
        converted = []
        try:
            for tx in txs:
                inputs = []
                for inp in tx.get("inputs", []):
                    prev_out = inp.get("prev_out", {})
                    addr = prev_out.get("addr", "")
                    if addr:
                        inputs.append(
                            {
                                "addresses": [addr],
                                "output_value": prev_out.get("value", 0),
                            }
                        )

                outputs = []
                for out in tx.get("out", []):
                    addr = out.get("addr")
                    if addr:
                        outputs.append(
                            {"addresses": [addr], "value": out.get("value", 0)}
                        )

                converted_tx = {
                    "hash": tx.get("hash"),
                    "block_height": tx.get("block_height"),
                    "confirmations": tx.get("block_height", 0)
                    if tx.get("block_height")
                    else 0,
                    "confirmed": tx.get("time"),
                    "received": tx.get("time"),
                    "fees": tx.get("fee", 0),
                    "inputs": inputs,
                    "outputs": outputs,
                }
                converted.append(converted_tx)

            logger.info(
                f"✅ Converted {len(converted)} transactions from Blockchain.com format"
            )
            return converted
        except Exception as e:
            logger.error(f"Error converting Blockchain.com format: {e}", exc_info=True)
            return []

    async def process_transaction(self, address: str, tx: dict):
        """Process a single transaction"""
        tx_hash = tx.get("hash")

        if tx_hash in self.last_seen_txs.get(address, set()):
            return

        self.last_seen_txs[address].add(tx_hash)

        total_input = 0
        total_output = 0

        for inp in tx.get("inputs", []):
            for addr in inp.get("addresses", []):
                if addr == address:
                    total_input += inp.get("output_value", 0)

        for out in tx.get("outputs", []):
            for addr in out.get("addresses", []):
                if addr == address:
                    total_output += out.get("value", 0)

        net_amount = total_output - total_input
        tx_type = "incoming" if net_amount > 0 else "outgoing"

        amount_btc = abs(net_amount) / 100_000_000

        event = {
            "contract_id": f"bitcoin-{address}",
            "event_type": "Transaction",
            "data": {
                "tx_hash": tx_hash,
                "block_height": tx.get("block_height"),
                "confirmations": tx.get("confirmations", 0),
                "type": tx_type,
                "amount_satoshis": abs(net_amount),
                "amount_btc": amount_btc,
                "total_input": total_input,
                "total_output": total_output,
                "confirmed": tx.get("confirmed", None),
                "received": tx.get("received", None),
                "fees": tx.get("fees", 0),
            },
        }

        await publish_event(event)

        direction = "received" if net_amount > 0 else "sent"
        logger.info(
            f"💰 Bitcoin {tx_type}: {amount_btc:.8f} BTC {direction} | "
            f"TX: {tx_hash[:16]}... | "
            f"Block: {tx.get('block_height', 'pending')} | "
            f"Address: {address[:16]}..."
        )

    async def poll_address(self, address: str):
        """Poll a single address for new transactions"""
        logger.info(f"🔍 Polling Bitcoin address: {address}")

        transactions = await self.fetch_address_transactions(address)

        if transactions:
            logger.info(f"Found {len(transactions)} transaction(s) for {address}")

            for tx in transactions:
                await self.process_transaction(address, tx)
        else:
            logger.debug(f"No transactions found for {address}")

    async def start_monitoring(self):
        """Main monitoring loop"""
        logger.info("🚀 Bitcoin Listener STARTED")

        while True:
            try:
                if not self.tracked_addresses:
                    logger.debug("No Bitcoin addresses to monitor yet...")
                    await asyncio.sleep(30)
                    continue

                for address in list(self.tracked_addresses):
                    await self.poll_address(address)

                await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"❌ Bitcoin listener error: {e}", exc_info=True)
                await asyncio.sleep(10)


_bitcoin_listener = None


def get_bitcoin_listener():
    """Get or create the Bitcoin listener singleton"""
    global _bitcoin_listener
    if _bitcoin_listener is None:
        _bitcoin_listener = BitcoinListener()
    return _bitcoin_listener


async def track_btc(address: str):
    """Start tracking a Bitcoin address"""
    listener = get_bitcoin_listener()
    listener.add_address(address)
    logger.info(f"✅ Added {address} to Bitcoin tracking list")


async def start_bitcoin_listener():
    """Start the Bitcoin listener (called from main.py)"""
    listener = get_bitcoin_listener()
    await listener.start_monitoring()
