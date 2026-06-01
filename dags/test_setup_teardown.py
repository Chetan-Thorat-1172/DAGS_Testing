"""
Test DAG: Setup/Teardown Tasks (#50)
====================================
Tests that:
1. Setup task runs first (creates resource)
2. Work tasks run after setup
3. Teardown task runs AFTER work tasks, even if work fails
4. Teardown runs regardless of work task failure

Flow: setup_cluster >> [process_data, FAILING_task] >> destroy_cluster(teardown)
Expected: setup succeeds, process_data succeeds, failing_task FAILS, destroy_cluster STILL RUNS
"""

from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime
import time


def setup_cluster(**context):
    print("SETUP: Creating compute cluster...")
    time.sleep(2)
    print("SETUP: Cluster created!")
    return {"cluster_id": "test-cluster-001"}


def process_data(**context):
    print("WORK: Processing data on cluster...")
    time.sleep(3)
    print("WORK: Data processed successfully!")
    return {"rows_processed": 1000}


def failing_task(**context):
    print("WORK: This task will fail intentionally...")
    time.sleep(1)
    raise Exception("Intentional failure to test teardown execution")


def destroy_cluster(**context):
    print("TEARDOWN: Destroying compute cluster...")
    time.sleep(2)
    print("TEARDOWN: Cluster destroyed! (should run even after failure)")
    return {"status": "cluster_destroyed"}


with DAG(
    dag_id="test_setup_teardown",
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Test: setup runs first, teardown runs last (even after failure)",
) as dag:

    setup = PythonOperator(
        task_id="setup_cluster",
        python_callable=setup_cluster,
        is_setup=True,
    )

    work1 = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    work2 = PythonOperator(
        task_id="failing_task",
        python_callable=failing_task,
    )

    teardown = PythonOperator(
        task_id="destroy_cluster",
        python_callable=destroy_cluster,
        is_teardown=True,
        trigger_rule="all_done",
    )

    setup >> [work1, work2] >> teardown
