"""
Test DAG: Dataset Producer (Session 49 - Dataset-Driven Scheduling)
Produces dataset events for BOTH datasets when tasks complete.
The consumer DAGs should auto-trigger:
  - test_dataset_consumer (basic, single dataset) → triggers
  - test_dataset_consumer_all (needs both) → triggers
  - test_dataset_consumer_any (needs either) → triggers
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator, Dataset

SALES_DATASET = Dataset("test://sales_table")
SESSIONS_DATASET = Dataset("test://sessions_table")

with DAG(
    dag_id="test_dataset_producer",
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Produces dataset events to trigger consumer DAGs",
    tags=["test", "dataset"],
) as dag:

    produce_sales = BashOperator(
        task_id="produce_sales",
        bash_command="echo 'Producing data for test://sales_table'",
        outlets=[SALES_DATASET],
    )

    produce_sessions = BashOperator(
        task_id="produce_sessions",
        bash_command="echo 'Producing data for test://sessions_table'",
        outlets=[SESSIONS_DATASET],
    )

    produce_sales >> produce_sessions
