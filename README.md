# Orders on the Move: Data at Rest and in Motion with Spark, Kafka and CDC

Start with **PROJECT.md**: what is provided, how to start the lab, the commands and the rules the checks use.

The lab (`docker compose up -d --build --wait`) runs four services:

- `workspace`: Python 3.12, Java 17, PySpark 3.5.9, Delta Lake 3.3, confluent-kafka, psycopg, JupyterLab,
  pytest and ruff. Open a terminal in it with `docker compose exec workspace bash`; this folder is
  `/workspace`. JupyterLab is at <http://127.0.0.1:8888> (token `projlab`), the Spark UI at
  <http://127.0.0.1:4040> while a Spark command runs.
- `postgres`: PostgreSQL 16 with logical replication: Fernhill's order system, schema `shop`.
- `kafka`: Apache Kafka 4.3, one broker in KRaft mode, at `kafka:9092`.
- `connect`: Kafka Connect with Debezium 3.6, at `http://connect:8083`.

Recommended machine: 4 CPUs and 8 GB of memory available to Docker. Stop the lab with `docker compose stop`;
`docker compose down -v` deletes its data (the database, Kafka's topics), so the day can start again.

Your code goes in `fernhill_move/`, your answers in `answers/`, the files your lab writes for the checks in
`evidence/`; the commands are in the `Makefile` (`make` lists them).
