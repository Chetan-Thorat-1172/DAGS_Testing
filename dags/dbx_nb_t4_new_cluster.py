from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksNotebookOperator

# T4: new_cluster path. Spins up a one-time job cluster for the run (no
# existing_cluster_id), mirroring Airflow's DatabricksNotebookOperator new_cluster
# example. PASS = success + run_id / run_page_url XComs.
WORKSPACE = "/Workspace/Users/ovaizsce121@siesgst.ac.in"

with DAG(
    dag_id="dbx_nb_t4_new_cluster",
    schedule=None,
    start_date=datetime(2026, 8, 14),
    catchup=False,
    default_args={"connection_id": "databricks_azure", "retries": 0},
    tags=["databricks", "notebook"],
) as dag:
    DatabricksNotebookOperator(
        task_id="run_new_cluster",
        notebook_path=f"{WORKSPACE}/maestro_smoke",
        source="WORKSPACE",
        new_cluster={
            "spark_version": "17.3.x-scala2.13",
            "node_type_id": "Standard_D4s_v3",
            "num_workers": 0,
            "spark_conf": {
                "spark.master": "local[*]",
                "spark.databricks.cluster.profile": "singleNode",
            },
            "custom_tags": {"ResourceClass": "SingleNode"},
        },
        notebook_params={"ds": "{{ .DS }}"},
        timeout_seconds=1800,
        poll_interval=15,
    )
