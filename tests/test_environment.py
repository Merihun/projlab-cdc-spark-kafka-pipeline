"""Verify your lab. Run inside the workspace: python -m pytest -q tests/test_environment.py"""

import json
import os
import shutil
import subprocess
import sys
import urllib.request

import pytest

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONNECT = os.environ.get("CONNECT_URL", "http://localhost:8083")


def test_python_version():
    assert sys.version_info[:2] == (3, 12), sys.version


def test_packages_import():
    import confluent_kafka  # noqa: F401
    import delta  # noqa: F401
    import psycopg  # noqa: F401
    import pyspark

    assert pyspark.__version__ == "3.5.9", pyspark.__version__


def test_java_17_is_available():
    java = shutil.which("java")
    assert java, "java not found; rebuild the container"
    out = subprocess.run([java, "-version"], capture_output=True, text=True).stderr
    assert '"17.' in out, out


@pytest.mark.service
def test_postgres_has_logical_replication(pg_conninfo):
    import psycopg

    with psycopg.connect(**pg_conninfo) as conn:
        assert conn.execute("SHOW wal_level").fetchone()[0] == "logical", "postgres must run with wal_level=logical"
        tables = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'shop'").fetchone()[0]
    assert tables == 5, "schema shop is missing its tables; reset with: docker compose down -v"


@pytest.mark.service
def test_kafka_broker_metadata():
    from confluent_kafka.admin import AdminClient

    metadata = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=10)
    assert len(metadata.brokers) == 1, metadata.brokers


@pytest.mark.service
def test_kafka_connect_has_the_debezium_postgres_connector():
    with urllib.request.urlopen(f"{CONNECT}/connector-plugins", timeout=10) as response:  # noqa: S310
        plugins = {p["class"] for p in json.load(response)}
    assert "io.debezium.connector.postgresql.PostgresConnector" in plugins, plugins
