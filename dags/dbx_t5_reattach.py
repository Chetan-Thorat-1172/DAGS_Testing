from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSubmitRunOperator

with DAG(
    dag_id="databricks_t5_reattach",
    schedule=None,
    start_date=datetime(2026, 8, 10),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 2,
    },
    tags=["databricks"],
) as dag:
    DatabricksSubmitRunOperator(
        task_id="sleepy",
        existing_cluster_id="0810-102016-bcimmln4",
        notebook_task={
            "notebook_path": "/Workspace/Users/ovaizsce121@siesgst.ac.in/maestro_sleep",
        },
        timeout_seconds=1800,
        polling_period_seconds=10,
    )
