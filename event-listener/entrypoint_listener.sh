#!/bin/bash
set -e

echo "Starting ChainHook Web3 Listener (USDC Sepolia)"
export PYTHONPATH="/app/event-listener:$PYTHONPATH"
cd /app/event-listener

exec uvicorn src.main:app --host 0.0.0.0 --port 8001