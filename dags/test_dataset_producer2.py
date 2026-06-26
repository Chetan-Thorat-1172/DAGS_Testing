from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator, Dataset


sales_data = Dataset("s3://analytics/sales_daily.parquet")


with DAG(
    dag_id="test_dataset_producer2",
    schedule_interval=None,
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:

    t_write = BashOperator(
        task_id="write_data",
        bash_command="echo 'writing sales data'",
        outlets=[sales_data],
    )
