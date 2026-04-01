"""Backward-compatible ASGI entry: `uvicorn api:app` (project root on PYTHONPATH)."""
from backend.main import app

__all__ = ["app"]
