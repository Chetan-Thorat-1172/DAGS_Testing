from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    BashOperator,
    BranchPythonOperator,
)


def branch_single(**kwargs):
    """Returns a single task_id"""
    return "path_a"


def branch_multi(**kwargs):
    """Returns multiple task_ids"""
    return ["path_x", "path_z"]


def branch_with_context(**kwargs):
    """Uses execution context to decide"""
    ds = kwargs.get("ds", "")
    return "path_even" if int(ds.replace("-", "")) % 2 == 0 else "path_odd"


def branch_empty():
    """Returns empty string — should FAIL (PI-FLOW behavior differs from Airflow)"""
    return ""


with DAG(
    dag_id="test_branch_executor",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 25),
    catchup=False,
) as dag:

    # --- Test 1: Single path selection ---
    b1 = BranchPythonOperator(
        task_id="b1_single",
        python_callable=branch_single,
    )

    path_a = BashOperator(
        task_id="path_a",
        bash_command="echo path_a selected",
    )

    path_b = BashOperator(
        task_id="path_b",
        bash_command="echo path_b skipped",
    )

    join1 = BashOperator(
        task_id="join1",
        bash_command="echo join1",
        trigger_rule="none_failed_min_one_success",
    )

    # --- Test 2: Multi-path selection ---
    b2 = BranchPythonOperator(
        task_id="b2_multi",
        python_callable=branch_multi,
    )

    path_x = BashOperator(
        task_id="path_x",
        bash_command="echo path_x selected",
    )

    path_y = BashOperator(
        task_id="path_y",
        bash_command="echo path_y skipped",
    )

    path_z = BashOperator(
        task_id="path_z",
        bash_command="echo path_z selected",
    )

    # --- Test 3: Context-aware branch ---
    b3 = BranchPythonOperator(
        task_id="b3_context",
        python_callable=branch_with_context,
    )

    path_even = BashOperator(
        task_id="path_even",
        bash_command="echo even date",
    )

    path_odd = BashOperator(
        task_id="path_odd",
        bash_command="echo odd date",
    )

    # --- Test 4: Empty return → should FAIL ---
    b4 = BranchPythonOperator(
        task_id="b4_empty",
        python_callable=branch_empty,
        retries=0,
    )

    # Dependencies
    b1 >> [path_a, path_b] >> join1
    b2 >> [path_x, path_y, path_z]
    b3 >> [path_even, path_odd]
