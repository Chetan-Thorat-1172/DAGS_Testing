from datetime import datetime
#
from dag_parser.dynamic.dag_context import DAG, BashOperator, DailyPartition


with DAG(
    dag_id="test_daily_partition",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    partitions=DailyPartition(),
) as dag:
    BashOperator(
        task_id="run",
        bash_command="echo daily partition run",
    )
