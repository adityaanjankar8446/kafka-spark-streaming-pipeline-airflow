from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta


JARS = ",".join([
    "/opt/jars/delta-spark_2.12-3.2.0.jar",
    "/opt/jars/delta-storage-3.2.0.jar",
    "/opt/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
    "/opt/jars/kafka-clients-3.4.1.jar",
    "/opt/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
    "/opt/jars/commons-pool2-2.11.1.jar",
])

PROJECT_DIR = "/d/Streaming_kafka_project"

SPARK_SUBMIT = (
    "docker run --rm --network streaming_kafka_project_sdp-net "
    f"--volume {PROJECT_DIR}/pipelines:/opt/pipelines "
    f"--volume {PROJECT_DIR}/data:/opt/data "
    f"--volume {PROJECT_DIR}/checkpoints:/opt/checkpoints "
    f"--volume {PROJECT_DIR}/jars:/opt/jars "
    "apache/spark:3.5.1 "
    "/opt/spark/bin/spark-submit "
    "--master spark://sdp-spark-master:7077 "
    "--executor-cores 1 "
    "--executor-memory 512m "
    "--total-executor-cores 3 "
    f"--jars {JARS} "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
)

# ── DAG definition ────────────────────────────────────────────────────────────
default_args = {
    "owner":            "dev",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id       = "e-commerce_kafka_streaming_pipeline_v10",
    description  = "Kafka → Bronze → Silver → Gold via Spark",
    default_args = default_args,
    start_date   = datetime(2024, 1, 1),
    schedule     = "0 * * * *",
    catchup      = False,
    tags         = ["kafka", "bronze", "silver", "gold"],
) as dag:

    # ── Task 1: Run Kafka producer (runs directly inside Airflow container) ───
    
    produce_messages = BashOperator(
        task_id      = "produce_messages",
        bash_command = "python3 /opt/pipelines/api_to_kafka.py",
    )

    # ── Task 2: Bronze — Kafka → raw Delta (run for 90s then stop) ───────────
    run_bronze = BashOperator(
        task_id      = "run_bronze",
        bash_command = (
            f"timeout 120s {SPARK_SUBMIT} /opt/pipelines/bronze_runner.py"
        ),
    )

    # ── Task 3: Wait until Bronze data exists ────────────────────────────────
    wait_for_bronze = FileSensor(
        task_id       = "wait_for_bronze",
        filepath      = "/opt/data/bronze/orders",
        fs_conn_id    = "fs_default",
        poke_interval = 15,
        timeout       = 300,
        mode          = "poke",
    )

    # ── Task 4: Silver — Bronze Delta → typed Delta ───────────────────────────
    run_silver = BashOperator(
        task_id      = "run_silver",
        bash_command = (
            f"timeout 120s {SPARK_SUBMIT} /opt/pipelines/silver_runner.py"
        ),
    )

    # ── Task 5: Wait until Silver data exists ────────────────────────────────
    wait_for_silver = FileSensor(
        task_id       = "wait_for_silver",
        filepath      = "/opt/data/silver/orders",
        fs_conn_id    = "fs_default",
        poke_interval = 15,
        timeout       = 300,
        mode          = "poke",
    )

    # ── Task 6: Gold — Silver Delta → aggregations ────────────────────────────
    run_gold = BashOperator(
        task_id      = "run_gold",
        bash_command = f"{SPARK_SUBMIT} /opt/pipelines/gold_runner.py",
    )

    # ── Task 7: Verify Gold output ────────────────────────────────────────────
    verify_gold = BashOperator(
        task_id      = "verify_gold",
        bash_command = """
            echo "=== Gold tables ==="
            ls -lh /opt/data/gold/ || echo "Gold dir not found"
            echo "=== Daily Sales rows ==="
            ls /opt/data/gold/daily_sales/ 2>/dev/null | wc -l || echo "0"
            echo "Pipeline complete!"
        """,
    )

    # ── Pipeline order ────────────────────────────────────────────────────────
    produce_messages >> run_bronze >> wait_for_bronze >> run_silver >> wait_for_silver >> run_gold >> verify_gold