from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


def load_project_env() -> None:
    """Load environment variables from project .env if present."""
    root = Path(__file__).resolve().parents[1]
    env_path = root / ".env"
    load_dotenv(dotenv_path=env_path, override=False)
