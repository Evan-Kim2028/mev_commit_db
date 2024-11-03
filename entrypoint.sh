#!/bin/bash
set -e

# Activate the virtual environment
source /app/.venv/bin/activate

# Set PYTHONPATH
export PYTHONPATH=/app

# Run the appropriate command based on the service
if [[ "$1" == "pipeline" ]]; then
    # Run the scripts for the pipeline service
    python /app/scripts/get_mev_commit_logs.py
    python /app/scripts/get_txs.py
elif [[ "$1" == "api" ]]; then
    # Start the FastAPI server for the api service
    exec uvicorn api.main:app --host 0.0.0.0 --port 8000
else
    # Fallback for any other commands passed
    exec "$@"
fi
