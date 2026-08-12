from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksRunNowOperator

# T4: cancel_previous_runs.
#
# Task A fires the job without waiting (so the Databricks run is still in
# flight when Task B starts).  Task B fires the same job with
# cancel_previous_runs=True — which calls jobs/runs/cancel-all on the job
# before triggering a new run, cancelling Task A's run.
#
# How to read the result:
#   Task A  → Databricks run will be CANCELLED by Task B (state: CANCELLED).
#   Task B  → should succeed.
#
# In Databricks Run history you should see two runs close together: the first
# showing CANCELLED and the second showing SUCCESS.
with DAG(
    dag_id="databricks_rn_t4_cancel",
    schedule=None,
    start_date=datetime(2026, 8, 12),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 0,
    },
    tags=["databricks", "run-now"],
) as dag:
    fire_first = DatabricksRunNowOperator(
        task_id="fire_without_waiting",
        job_id=632128058992973,
        wait_for_termination=False,  # returns immediately; run still in flight
        polling_period_seconds=10,
    )

    cancel_and_retrigger = DatabricksRunNowOperator(
        task_id="cancel_and_retrigger",
        job_id=632128058992973,
        cancel_previous_runs=True,   # cancels the run started by fire_first
        polling_period_seconds=10,
    )

    fire_first >> cancel_and_retrigger
