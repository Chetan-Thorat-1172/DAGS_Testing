from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    BashOperator,
    DailyPartition,
)


with DAG(
    dag_id="test_partitions",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 10),
    catchup=True,
    partitions=WeeklyPartition(),
) as dag:

    t1 = BashOperator(
        task_id="process_partition",
        bash_command="echo processing partition",
    )
