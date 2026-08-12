from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksRunNowOperator

# T5: wait_for_termination=False — fire and forget.
#
# The task succeeds immediately after the run-now API call returns, without
# polling. The Databricks run will still execute in the background.
#
# What to check:
#   - The task finishes very quickly (a few seconds, not the run duration).
#   - run_id and run_page_url are pushed as XComs even without waiting.
#   - The Databricks run eventually finishes on its own.
with DAG(
    dag_id="databricks_rn_t5_no_wait",
    schedule=None,
    start_date=datetime(2026, 8, 12),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 0,
    },
    tags=["databricks", "run-now"],
) as dag:
    DatabricksRunNowOperator(
        task_id="fire_and_forget",
        job_id=632128058992973,
        wait_for_termination=False,
        job_parameters={"run_label": "no_wait_test"},
    )
