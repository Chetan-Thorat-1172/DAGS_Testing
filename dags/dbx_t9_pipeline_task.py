"""T9 — pipeline_task with no compute declared.

A Lakeflow/DLT pipeline brings its own compute, so the submit must carry NO
cluster at all. This is the case the operator must not "help" with: if anything
injects a top-level existing_cluster_id here, Databricks rejects the payload.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, DatabricksSubmitRunOperator

PIPELINE_ID = "df7c8111-23ac-4753-a53c-3fb652b32c9a"

with DAG(
    dag_id="databricks_t9_pipeline_task",
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
        task_id="dlt_pipeline",
        pipeline_task={
            "pipeline_id": PIPELINE_ID,
        },
        timeout_seconds=3600,
        polling_period_seconds=15,
    )
