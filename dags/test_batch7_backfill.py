"""
Test DAG for Batch 7 — Backfill API + Partition Support + DAG Serialization.

This DAG:
- Runs daily at midnight (schedule_interval="@daily")
- Uses DailyPartition to enable partition tracking
- Has a simple PythonOperator task that prints the execution date
- Is suitable for testing POST /api/dags/:dag_id/backfill
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DailyPartition
from dag_parser.dynamic.operators import PythonOperator


def process_partition(**context):
    """Simulates processing a daily partition."""
    exec_date = context.get("ds", "unknown")
    print(f"Processing partition for date: {exec_date}")
    return {"partition_date": exec_date, "status": "processed"}


with DAG(
    dag_id="test_batch7_backfill",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    description="Batch 7 test: backfill + partitions + serialization",
    tags=["batch7", "test", "backfill", "partition"],
    partitions=DailyPartition(),
) as dag:

    process = PythonOperator(
        task_id="process_daily_data",
        python_callable=process_partition,
    )
