"""Read-only preflight for the Snowpark Container Services live test.

Reports what exists so the test DAGs are written against fact rather than
assumption: the compute pool and its state, the images actually in the
repository, and whether a spec file has been staged. Creates nothing.
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    SQLExecuteQueryOperator,
)

CONN = "snowflake_conn"


def report(**context):
    ti = context["ti"]
    for task_id in ("compute_pools", "images", "stage_files", "grants"):
        raw = ti.xcom_pull(task_ids=task_id, key="return_value")
        rows = json.loads(raw) if isinstance(raw, str) else raw
        print(f"\n===== {task_id} =====")
        if not rows:
            print("(nothing returned)")
            continue
        for r in rows:
            print(r)
    return "ok"


with DAG(
    dag_id="snowpark_preflight",
    description="Read-only check of the Snowpark Container Services setup",
    tags=["livetest", "snowflake", "spcs"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # The pool must exist; SUSPENDED is fine when AUTO_RESUME is on, because
    # submitting a job resumes it.
    compute_pools = SQLExecuteQueryOperator(
        task_id="compute_pools",
        sql="SHOW COMPUTE POOLS LIKE 'SPCSTEST_POOL'",
        show_return_value_in_logs=True,
    )

    # The image has to be in Snowflake's own repository — SPCS cannot pull from
    # Docker Hub.
    images = SQLExecuteQueryOperator(
        task_id="images",
        sql="SHOW IMAGES IN IMAGE REPOSITORY DEMO.PUBLIC.SPCSTEST_REPO",
        show_return_value_in_logs=True,
    )

    # Only needed for the staged-spec path.
    stage_files = SQLExecuteQueryOperator(
        task_id="stage_files",
        sql="LIST @DEMO.PUBLIC.SPCSTEST_STAGE",
        show_return_value_in_logs=True,
    )

    # Whether this role can actually use the pool.
    grants = SQLExecuteQueryOperator(
        task_id="grants",
        sql="SELECT CURRENT_ROLE() AS ROLE, CURRENT_ACCOUNT() AS ACCOUNT",
        show_return_value_in_logs=True,
    )

    [compute_pools, images, stage_files, grants] >> PythonOperator(
        task_id="report",
        python_callable=report,
        provide_context=True,
    )
