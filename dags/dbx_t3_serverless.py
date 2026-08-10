from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSubmitRunOperator


with DAG(
    dag_id="databricks_t3_serverless",
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
        task_id="notebook_serverless",
        notebook_task={
            "notebook_path": "/Workspace/Users/ovaizsce121@siesgst.ac.in/maestro_smoke",
            "base_parameters": {
                "ds": "{{ .DS }}",
            },
        },
        timeout_seconds=1200,
        polling_period_seconds=10,
    )
