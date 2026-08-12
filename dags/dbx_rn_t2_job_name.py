from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksRunNowOperator

# T2: trigger by job_name instead of job_id.
# Proves FindJobIDByName: the operator calls jobs/list with the name filter,
# applies the exact client-side match, and resolves to the ID before calling
# run-now. If there were two jobs named "maestro_runnow_test" this task would
# fail with "found 2 jobs" rather than picking one.
with DAG(
    dag_id="databricks_rn_t2_job_name",
    schedule=None,
    start_date=datetime(2026, 8, 12),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 1,
    },
    tags=["databricks", "run-now"],
) as dag:
    DatabricksRunNowOperator(
        task_id="trigger_by_name",
        job_name="maestro_runnow_test",
        polling_period_seconds=10,
    )
