from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator, Dataset


sales_data = Dataset("s3://analytics/sales_daily.parquet")


with DAG(
    dag_id="test_dataset_consumer2",
    schedule=[sales_data],
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:

    t_process = BashOperator(
        task_id="process_data",
        bash_command="echo 'consumer triggered - processing sales data'",
    )
