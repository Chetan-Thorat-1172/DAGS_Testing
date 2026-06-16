"""
Regression: mid-flight DAG version pinning.

VERSION 1: start >> slow >> finish   (slow sleeps ~60s to keep the run in-flight)

Protocol:
  1. trigger a run (pins v1 dag_hash).
  2. while 'slow' is running, push VERSION 2 which ADDS task 'extra'
     (finish >> extra). Ingestion registers v2 as the latest version.
  3. the in-flight run MUST finish using v1: instances = {start, slow, finish}
     with NO 'extra' instance, run -> success, run.dag_hash stays = v1.
     A NEW run would include 'extra'.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ok(**context):
    return "ok"


def slow(**context):
    import time
    time.sleep(60)
    return "slow_done"


with DAG(
    dag_id="rt_midflight",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression: mid-flight version pinning (v1: start>>slow>>finish)",
    tags=["regression", "cat2", "versioning"],
) as dag:
    start = PythonOperator(task_id="start", python_callable=ok)
    slow_t = PythonOperator(task_id="slow", python_callable=slow)
    finish = PythonOperator(task_id="finish", python_callable=ok)

    start >> slow_t >> finish
