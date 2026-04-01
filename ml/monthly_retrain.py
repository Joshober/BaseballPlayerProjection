"""Monthly retrain hook — invoke from cron or APScheduler with DATABASE_URL set."""
from __future__ import annotations

from ml.train_all import main as train_main


def run() -> None:
    train_main()


if __name__ == "__main__":
    run()
