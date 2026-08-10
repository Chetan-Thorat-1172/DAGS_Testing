from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, DatabricksSubmitRunOperator


with DAG(
    dag_id="databricks_t4_json_merge",
    schedule=None,
    start_date=datetime(2026, 8, 10),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 1,
    },
    tags=["databricks"],
) as dag:
    DatabricksSubmitRunOperator(
        task_id="json_precedence",
        json={
            "existing_cluster_id": "should_be_overridden",
            "notebook_task": {
                "notebook_path": (
                    "/Workspace/Users/ovaizsce121@siesgst.ac.in/maestro_smoke"
                ),
                "base_parameters": {
                    "ds": "from_json",
                },
            },
        },
        existing_cluster_id="0810-102016-bcimmln4",
        timeout_seconds=1200,
    )
