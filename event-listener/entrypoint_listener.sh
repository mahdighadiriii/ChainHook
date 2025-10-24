#!/bin/bash
set -e

echo "Starting ChainHook Web3 Listener (USDC Sepolia)"
export PYTHONPATH="/app/event-listener:$PYTHONPATH"
cd /app/event-listener

# Add error handling and verbose output
python -u -m src.blockchain_listener 2>&1 || {
    echo "ERROR: blockchain_listener crashed with exit code $?"
    sleep 5
    exit 1
}