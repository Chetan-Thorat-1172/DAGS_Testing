from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksNotebookOperator

# T3: failure path. EXPECTED TO GO RED — that is the pass condition.
# The notebook path does not exist, so the run reaches a non-success terminal
# state and the task fails, surfacing the Databricks reason. retries=0.
WORKSPACE = "/Workspace/Users/ovaizsce121@siesgst.ac.in"
CLUSTER = "0810-102016-bcimmln4"

with DAG(
    dag_id="dbx_nb_t3_failure",
    schedule=None,
    start_date=datetime(2026, 8, 14),
    catchup=False,
    default_args={"connection_id": "databricks_azure", "retries": 0},
    tags=["databricks", "notebook"],
) as dag:
    DatabricksNotebookOperator(
        task_id="missing_notebook",
        notebook_path=f"{WORKSPACE}/a_notebook_that_does_not_exist",
        source="WORKSPACE",
        existing_cluster_id=CLUSTER,
        timeout_seconds=600,
        poll_interval=10,
    )
