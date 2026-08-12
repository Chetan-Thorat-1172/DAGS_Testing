from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksRunNowOperator

# T6: durable reattach — the duplicate-trigger guard.
#
# This DAG has retries=2 and the task is exactly the same as T1.  To test
# reattach manually:
#
#   1. Trigger this DAG. The first attempt fires the job and writes the
#      run_id to XCom under "databricks_run_now_id".
#   2. While the Databricks run is still executing, kill the worker
#      container and restart it (or wait for the task to fail on its own,
#      e.g. by inducing a polling error).
#   3. The retry must pick up the SAME run (same run_id in Databricks) rather
#      than triggering a second one.
#
# How to verify:
#   - Check the XCom "databricks_run_now_id" in the Maestro UI — it should
#     appear on attempt 1 and still be present on attempt 2.
#   - In Databricks Run history there should be only ONE run for this trigger,
#     not two.
#
# Quick path (no worker kill needed): trigger the DAG twice in quick
# succession on the same run_id. On the second trigger the task will be on
# attempt 1 again (new run_id in the DAG run), so it submits fresh — which
# is also the correct behaviour for a new DAG run.
#
# To observe the reattach path without killing Docker, see the operational
# notes in the DAG authoring guide (section 60, durable parameter row).
with DAG(
    dag_id="databricks_rn_t6_durable",
    schedule=None,
    start_date=datetime(2026, 8, 12),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 2,
        "retry_delay": 5,
    },
    tags=["databricks", "run-now"],
) as dag:
    DatabricksRunNowOperator(
        task_id="durable_trigger",
        job_id=632128058992973,
        durable=True,          # default, written explicitly to make intent clear
        polling_period_seconds=10,
    )
