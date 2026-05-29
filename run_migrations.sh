#!/bin/bash
set -e

echo "Running database migrations..."
# Since we installed to --system, alembic is in the PATH
alembic upgrade head

echo "Starting FastAPI server..."
exec uvicorn main:app --host 0.0.0.0 --port 8000
