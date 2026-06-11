"""
Test DAG: Partition Status Update (Session 2 Scheduler Fix)

Validates that GetDAGByID() now returns PARTITION_DEF, enabling
dag_run_finalizer to correctly update partition_status on run completion.

Expected: after the run completes, GET /api/dags/test_partition_status/partitions
returns a non-empty partition list with state=success.
"""
from datetime import datetime, timedelta
from dag_parser.dynamic.dag_context import DAG, DailyPartition
from dag_parser.dynamic.operators import PythonOperator


def process_data(**context):
    """Simple task that returns data to verify execution."""
    exec_date = context.get("ds", "unknown")
    print(f"Processing partition for: {exec_date}")
    return {"date": exec_date, "status": "ok"}


with DAG(
    dag_id="test_partition_status",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 10),
    catchup=False,
    description="Validates PARTITION_DEF fix in GetDAGByID (Session 2)",
    tags=["test", "scheduler", "partition"],
    partitions=DailyPartition(),
) as dag:

    process = PythonOperator(
        task_id="process_partition_data",
        python_callable=process_data,
    )
