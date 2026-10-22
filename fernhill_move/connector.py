"""Step 4: the Debezium connector, registered through Kafka Connect's REST API."""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from fernhill_move import settings

PLACEHOLDER = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")


def load_config(path: Path | str = settings.CONNECTOR_FILE, env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """The connector file ({"name": ..., "config": {...}}) with every ${NAME} in a string value replaced by
    env[NAME] (env defaults to os.environ), so no password is written in the file. A variable that is not set
    is a KeyError naming it."""
    raise NotImplementedError("step 4: load_config")


def _call(method: str, url: str, body: dict | None = None) -> tuple[int, Any]:
    """One request to Kafka Connect: (HTTP status, the JSON it answered). Provided."""
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json",
                                                                             "Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - the lab's own Connect
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"null")


def register(document: dict[str, Any], url: str = settings.CONNECT_URL) -> dict[str, Any]:
    """Create the connector, or update it when it exists, in one call that is safe to repeat: PUT
    /connectors/<name>/config with the config object as the body. Returns Connect's answer; when Connect
    refuses (a status other than 200 or 201), a RuntimeError with its message."""
    raise NotImplementedError("step 4: register")


def connector_status(name: str, url: str = settings.CONNECT_URL) -> dict[str, Any]:
    """GET /connectors/<name>/status: the connector's state and its tasks' states. RuntimeError unless 200."""
    raise NotImplementedError("step 4: connector_status")
