"""T10 — deliberate reproduction of the default-cluster injection defect.

Same pipeline_task as T9, but with `cluster_id` set. `cluster_id` is the legacy
Maestro alias that resolves through the SAME line as a connection's stored
default cluster:

    firstNonEmptyString(params.ClusterID, connDefaultCluster)

so this reproduces, without needing a second connection, what any consumer would
hit if they set a default cluster on their Databricks connection and then wrote a
pipeline_task.

EXPECTED RESULT: failure. We want Databricks' exact error text to drive the fix,
rather than fixing on assumption — the same method that corrected the earlier
serverless guess.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, DatabricksSubmitRunOperator

PIPELINE_ID = "df7c8111-23ac-4753-a53c-3fb652b32c9a"
CLUSTER = "0810-102016-bcimmln4"

with DAG(
    dag_id="databricks_t10_pipeline_cluster_injection",
    schedule=None,
    start_date=datetime(2026, 8, 11),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 0,
    },
    tags=["databricks"],
) as dag:
    DatabricksSubmitRunOperator(
        task_id="dlt_pipeline_with_cluster_id",
        cluster_id=CLUSTER,
        pipeline_task={
            "pipeline_id": PIPELINE_ID,
        },
        timeout_seconds=3600,
        polling_period_seconds=15,
    )
