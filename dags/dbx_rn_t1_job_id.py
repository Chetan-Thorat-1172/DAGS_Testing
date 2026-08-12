from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksRunNowOperator

# T1: simplest possible trigger — job_id, no extra parameters.
# Proves the basic Execute path works end-to-end: connection → RunNow API →
# poll → terminal state → task succeeds.
with DAG(
    dag_id="databricks_rn_t1_job_id",
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
        task_id="trigger_by_id",
        job_id=632128058992973,
        polling_period_seconds=10,
    )
