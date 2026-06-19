# Kafka-Spark Streaming Pipeline with Airflow Orchestration

An end-to-end streaming data pipeline that ingests e-commerce data (orders, customers, products) from a REST API into **Kafka**, processes it through a **Bronze → Silver → Gold** Delta Lake architecture using **Apache Spark Structured Streaming**, and orchestrates the entire flow with **Apache Airflow**. The whole stack runs locally via **Docker Compose**.

## Architecture

```
FakeStoreAPI ──> Kafka Producer ──> Kafka Topics ──> Spark Structured Streaming
                                                              │
                                                              ▼
                                          ┌──────────────────────────────────┐
                                          │   Bronze (raw JSON → Delta)      │
                                          └──────────────────────────────────┘
                                                              │
                                                              ▼
                                          ┌──────────────────────────────────┐
                                          │   Silver (parsed, typed, flat)   │
                                          └──────────────────────────────────┘
                                                              │
                                                              ▼
                                          ┌──────────────────────────────────┐
                                          │   Gold (business aggregations)   │
                                          └──────────────────────────────────┘

                  Orchestrated end-to-end by an Airflow DAG (hourly schedule)
```

### Medallion layers

| Layer | Purpose | Format |
|-------|---------|--------|
| **Bronze** | Raw Kafka messages landed as-is | Delta (JSON string column) |
| **Silver** | Parsed, flattened, and typed records per entity (orders, customers, products) | Delta |
| **Gold** | Business-level aggregations for analytics | Delta |

## Tech Stack

- **Apache Kafka** (Confluent images) — message broker for ingestion
- **Apache Spark 3.5.1** (Structured Streaming + Delta Lake) — stream processing across all three layers
- **Delta Lake** — storage format with ACID guarantees and schema merge support
- **Apache Airflow 2.9.0** — DAG-based orchestration of the producer and each Spark stage
- **Docker Compose** — local multi-container orchestration (Kafka, Zookeeper, Spark cluster, Airflow, Postgres)
- **Kafka UI** — web UI for inspecting topics and messages
- **PostgreSQL** — Airflow metadata database
- **Python** (`kafka-python`, `requests`, `pyspark`)

## Data Flow

1. **Producer** (`producer/`) pulls data from [FakeStoreAPI](https://fakestoreapi.com) (`/carts`, `/users`, `/products`) and publishes each record as JSON to the corresponding Kafka topic: `orders`, `customers`, `products`.
2. **Bronze** (`pipelines/bronze_runner.py`) subscribes to all three Kafka topics via Spark Structured Streaming and writes the raw JSON payloads to Delta tables, one per topic, with checkpointing for fault tolerance.
3. **Silver** (`pipelines/silver_runner.py`) reads each Bronze Delta table as a stream, parses the JSON against an explicit schema, flattens nested structures (e.g. exploding order line items, flattening customer address/geolocation), and writes typed Delta tables.
4. **Gold** (`pipelines/gold_runner.py`) reads Silver tables as static Delta reads and computes three aggregate tables:
   - **`daily_sales`** — total orders, revenue, and items sold per day
   - **`product_performance`** — quantity sold, revenue, and order count per product
   - **`customer_orders`** — total orders, total spend, and items purchased per customer
5. **Airflow DAG** (`dags/`) ties it all together hourly: run producer → run Bronze (time-boxed) → wait for Bronze data → run Silver (time-boxed) → wait for Silver data → run Gold → verify Gold output.

## Project Structure

```
Streaming_kafka_project/
├── aggregations/          # (Gold-layer aggregation logic / notebooks)
├── checkpoints/           # Spark Structured Streaming checkpoints (gitignored)
├── config/                # Configuration files
├── dags/                  # Airflow DAG definitions
│   └── sdp_pipeline_dag.py
├── data/                  # Delta Lake tables: bronze/, silver/, gold/ (gitignored)
├── jars/                  # Spark dependency JARs (Delta, Kafka connectors) (gitignored)
├── logs/                  # Airflow logs (gitignored)
├── pipelines/              # Spark Structured Streaming jobs
│   ├── bronze_runner.py
│   ├── silver_runner.py
│   └── gold_runner.py
├── producer/               # Kafka producer (API → Kafka)
│   └── api_to_kafka.py
├── scripts/                 # Helper/setup scripts
├── venv/                    # Python virtual environment (gitignored)
├── docker-compose.yml        # Full stack definition
└── requirements.txt           # Python dependencies
```

## Prerequisites

- Docker & Docker Compose
- Python 3.8+ (for local development outside containers, if needed)
- ~6 GB free RAM for the full stack (Spark workers + Airflow + Kafka + Postgres)

## Setup & Running

1. **Clone the repository**
   ```bash
   git clone https://github.com/adityaanjankar8446/kafka-spark-streaming-pipeline-airflow.git
   cd kafka-spark-streaming-pipeline-airflow
   ```

2. **Start the stack**
   ```bash
   docker-compose up -d
   ```
   This spins up: Zookeeper, Kafka broker, Kafka UI, Spark master + 2 workers, Postgres, and Airflow (init, scheduler, webserver).

3. **Access the UIs**
   - Airflow: [http://localhost:8082](http://localhost:8082) (default login: `admin` / `admin`)
   - Kafka UI: [http://localhost:8087](http://localhost:8087)
   - Spark Master UI: [http://localhost:9090](http://localhost:9090)

4. **Trigger the pipeline**
   In the Airflow UI, unpause and trigger the DAG `e-commerce_kafka_streaming_pipeline_v10`, or let it run on its hourly schedule.

5. **Verify output**
   The DAG's final task lists the Gold Delta tables and row counts. You can also inspect `data/gold/` directly, or query the Delta tables with Spark/`delta-rs`/any Delta-compatible reader.

## Airflow DAG Overview

DAG ID: `e-commerce_kafka_streaming_pipeline_v10` — runs hourly (`0 * * * *`), no catchup.

| Task | Description |
|------|-------------|
| `produce_messages` | Runs the producer script to pull fresh data from the API and publish to Kafka |
| `run_bronze` | Runs the Bronze Spark job inside a Docker container (time-boxed to 120s) |
| `wait_for_bronze` | `FileSensor` polling until Bronze Delta output exists |
| `run_silver` | Runs the Silver Spark job (time-boxed to 120s) |
| `wait_for_silver` | `FileSensor` polling until Silver Delta output exists |
| `run_gold` | Runs the Gold aggregation job |
| `verify_gold` | Lists Gold tables and row counts as a sanity check |

Each Spark stage is submitted via `spark-submit` inside an ephemeral `apache/spark:3.5.1` Docker container, connected to the `sdp-spark-master` cluster, with Delta Lake and Kafka connector JARs attached.

## Notes

- Bronze and Silver jobs are intentionally time-boxed (`timeout 120s`) in the DAG since they run as continuous streaming queries; this lets a single DAG run process a bounded micro-batch window rather than running forever.
- Kafka topics are auto-created (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: true`), so no manual topic setup is required.
- Checkpoints under `checkpoints/` are required for exactly-once / fault-tolerant streaming and are excluded from version control since they're environment-specific and regenerable.

## License

This project is provided as-is for educational and portfolio purposes.
