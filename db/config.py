from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


def load_project_env() -> None:
    """Load environment variables from project .env if present."""
    root = Path(__file__).resolve().parents[1]
    env_path = root / ".env"
    # Local `.env` should win over inherited shell/IDE env (e.g. stale DATABASE_URL).
    load_dotenv(dotenv_path=env_path, override=True)
