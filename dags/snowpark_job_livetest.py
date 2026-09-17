"""Live end-to-end test for SnowparkContainerJobOperator.

Runs alpine on DEMO.PUBLIC.SPCSTEST_POOL. The tasks are chained rather than
parallel: the pool has MAX_NODES = 1 and starts suspended, so the first task
waits for it to resume and the rest reuse it.

Two things are being settled here that could not be settled offline:

  * whether the staged-spec clause is SPEC = (what the reference emits) or
    SPECIFICATION_FILE = (what Snowflake's current documentation shows);
  * the exact shape of the EXECUTE JOB SERVICE response, which is where the job
    name is parsed from.

A green run is the assertion. Deliberate failures live in
snowpark_job_failures.py.
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    SnowparkContainerJobOperator,
    SQLExecuteQueryOperator,
)

CONN = "snowflake_conn"
SESSION = {"database": "DEMO", "schema": "PUBLIC"}
IMAGE = "/demo/public/spcstest_repo/alpine:3.20"

# The inline specification needs no stage, so it isolates the operator from the
# staged-spec question.
INLINE_SPEC = f"""spec:
  containers:
    - name: main
      image: {IMAGE}
      command:
        - sh
        - -c
        - echo hello from spcs; echo line two; exit 0
"""


def report(**context):
    """Record the job names, which is what the operator returns."""
    ti = context["ti"]
    for task_id in ("inline_spec", "staged_spec", "two_replicas", "no_wait", "kept_on_success"):
        raw = ti.xcom_pull(task_ids=task_id, key="return_value")
        print(f"{task_id} -> job name = {raw!r}")
        assert raw, f"{task_id} returned no job name"

    # do_xcom_push=False must leave nothing behind.
    suppressed = ti.xcom_pull(task_ids="no_xcom", key="return_value")
    print(f"no_xcom -> {suppressed!r}")
    assert suppressed is None, f"do_xcom_push=False still pushed {suppressed!r}"

    print("ALL ASSERTIONS PASSED")
    return "ok"


with DAG(
    dag_id="snowpark_job_livetest",
    description="Running a container job on Snowpark Container Services",
    tags=["livetest", "snowflake", "spcs"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # The plain case, with the specification inline. Generous timeout: the pool
    # starts suspended and has to resume first.
    inline_spec = SnowparkContainerJobOperator(
        task_id="inline_spec",
        compute_pool="SPCSTEST_POOL",
        container_name="main",
        spec_text=INLINE_SPEC,
        poll_interval=10,
        timeout=900,
        **SESSION,
    )

    # The documented example's shape: a specification file on a stage. This is
    # the task that settles SPEC = versus SPECIFICATION_FILE =.
    staged_spec = SnowparkContainerJobOperator(
        task_id="staged_spec",
        compute_pool="SPCSTEST_POOL",
        container_name="main",
        spec="job_spec.yaml",
        spec_stage="@DEMO.PUBLIC.SPCSTEST_STAGE",
        poll_interval=10,
        timeout=600,
        **SESSION,
    )

    # More than one replica, so more than one instance's logs are fetched.
    two_replicas = SnowparkContainerJobOperator(
        task_id="two_replicas",
        compute_pool="SPCSTEST_POOL",
        container_name="main",
        spec_text=INLINE_SPEC,
        replicas=2,
        poll_interval=10,
        timeout=600,
        **SESSION,
    )

    # Returns as soon as Snowflake accepts the job: no polling, no logs, and
    # nothing for drop_on_completion to act on.
    no_wait = SnowparkContainerJobOperator(
        task_id="no_wait",
        compute_pool="SPCSTEST_POOL",
        container_name="main",
        spec_text=INLINE_SPEC,
        wait_for_completion=False,
        **SESSION,
    )

    # A successful job that is deliberately NOT dropped.
    kept_on_success = SnowparkContainerJobOperator(
        task_id="kept_on_success",
        compute_pool="SPCSTEST_POOL",
        container_name="main",
        spec_text=INLINE_SPEC,
        drop_on_completion=False,
        poll_interval=10,
        timeout=600,
        **SESSION,
    )

    # The job name must not be pushed when the author asks for no XCom.
    no_xcom = SnowparkContainerJobOperator(
        task_id="no_xcom",
        compute_pool="SPCSTEST_POOL",
        container_name="main",
        spec_text=INLINE_SPEC,
        do_xcom_push=False,
        poll_interval=10,
        timeout=600,
        **SESSION,
    )

    # What survived: kept_on_success and the un-waited job should still exist,
    # the rest should have been dropped.
    surviving_services = SQLExecuteQueryOperator(
        task_id="surviving_services",
        sql="SHOW SERVICES IN SCHEMA DEMO.PUBLIC",
        show_return_value_in_logs=True,
    )

    (
        inline_spec >> staged_spec >> two_replicas >> no_wait
        >> kept_on_success >> no_xcom >> surviving_services
        >> PythonOperator(
            task_id="report",
            python_callable=report,
            provide_context=True,
        )
    )
