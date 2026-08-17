from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksNotebookOperator

# T1: happy path. Runs a real workspace notebook on an existing cluster and waits
# for it to finish. PASS = success, and the run_id / run_page_url XComs are pushed.
WORKSPACE = "/Workspace/Users/ovaizsce121@siesgst.ac.in"
CLUSTER = "0810-102016-bcimmln4"

with DAG(
    dag_id="dbx_nb_t1_smoke",
    schedule=None,
    start_date=datetime(2026, 8, 14),
    catchup=False,
    default_args={"connection_id": "databricks_azure", "retries": 0},
    tags=["databricks", "notebook"],
) as dag:
    DatabricksNotebookOperator(
        task_id="run_smoke",
        notebook_path=f"{WORKSPACE}/maestro_smoke",
        source="WORKSPACE",
        existing_cluster_id=CLUSTER,
        notebook_params={"ds": "{{ .DS }}"},
        timeout_seconds=1800,
        poll_interval=10,
    )
