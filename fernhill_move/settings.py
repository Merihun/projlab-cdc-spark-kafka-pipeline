"""Where things are. Provided: every value can be changed with the environment variable named next to it."""

from __future__ import annotations

import os
from pathlib import Path

# The lake: Parquet and Delta tables in a folder (data/ is git-ignored).
LAKE = Path(os.environ.get("FERNHILL_LAKE", "data/lake"))
CHECKPOINTS = Path(os.environ.get("FERNHILL_CHECKPOINTS", "data/checkpoints"))
BRONZE = LAKE / "bronze" / "changes"
SILVER = LAKE / "silver"
GOLD = LAKE / "gold"

# The lab's services, as compose.yaml names them inside the workspace.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
CONNECTOR_FILE = Path("connector/fernhill-shop.json")


def postgres() -> dict[str, str]:
    """The source database's settings (POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER,
    POSTGRES_PASSWORD), with the lab's defaults."""
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "dbname": os.environ.get("POSTGRES_DB", "projlab"),
        "user": os.environ.get("POSTGRES_USER", "projlab"),
        "password": os.environ.get("POSTGRES_PASSWORD", "projlab"),
    }
