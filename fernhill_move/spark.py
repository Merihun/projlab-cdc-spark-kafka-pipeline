"""A Spark session for this project. Provided.

Local mode, Delta Lake, the Kafka source and the PostgreSQL JDBC driver, and every time in UTC. The first
session downloads Delta Lake, the Kafka connector and the JDBC driver from Maven Central (a minute or two)
into data/ivy (SPARK_IVY_DIR), in the project folder, so a rebuilt workspace container doesn't download them
again. Every session still prints Ivy's resolution report (about 50 lines) before its own output.
"""

from __future__ import annotations

import os

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"
JDBC_PACKAGE = "org.postgresql:postgresql:42.7.13"


def get_spark(app_name: str = "fernhill-move") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.jars.ivy", os.path.abspath(os.environ.get("SPARK_IVY_DIR", "data/ivy")))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PACKAGE, JDBC_PACKAGE]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
