from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSubmitRunOperator


with DAG(
    dag_id="databricks_t2_new_cluster",
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
        task_id="notebook_new_cluster",
        new_cluster={
            "spark_version": "17.3.x-scala2.13",
            "node_type_id": "Standard_D4s_v3",
            "num_workers": 0,
            "spark_conf": {
                "spark.master": "local[*]",
                "spark.databricks.cluster.profile": "singleNode",
            },
            "custom_tags": {
                "ResourceClass": "SingleNode",
            },
        },
        notebook_task={
            "notebook_path": "/Workspace/Users/ovaizsce121@siesgst.ac.in/maestro_smoke",
            "base_parameters": {
                "ds": "{{ .DS }}",
            },
        },
        timeout_seconds=1800,
        polling_period_seconds=15,
    )
