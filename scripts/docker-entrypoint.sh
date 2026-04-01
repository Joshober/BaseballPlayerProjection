#!/bin/sh
set -e
cd /app/backend
alembic upgrade head
cd /app
exec uvicorn backend.main:app --host 0.0.0.0 --port 8000
