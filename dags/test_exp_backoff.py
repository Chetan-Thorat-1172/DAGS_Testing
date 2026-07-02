from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator

def always_fail(**kwargs):
    raise Exception("Simulated failure for backoff test")

with DAG(
    dag_id="test_exp_backoff",
    schedule_interval=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
) as dag:

    t_fail = PythonOperator(
        task_id="t_backoff",
        python_callable=always_fail,
        retries=4,
        retry_delay_seconds=10,
        retry_exponential_backoff=True,
        max_retry_delay_seconds=60,
    )
