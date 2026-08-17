from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksNotebookOperator

# T2: notebook_packages + return_value. Runs a notebook that imports `tabulate`
# and exits with its version via dbutils.notebook.exit(...). PASS = success AND a
# non-empty return_value XCom (proves the library was installed for the run and
# the notebook's exit value is surfaced).
WORKSPACE = "/Workspace/Users/ovaizsce121@siesgst.ac.in"
CLUSTER = "0810-102016-bcimmln4"

with DAG(
    dag_id="dbx_nb_t2_libraries",
    schedule=None,
    start_date=datetime(2026, 8, 14),
    catchup=False,
    default_args={"connection_id": "databricks_azure", "retries": 0},
    tags=["databricks", "notebook"],
) as dag:
    DatabricksNotebookOperator(
        task_id="run_lib_check",
        notebook_path=f"{WORKSPACE}/maestro_lib_check",
        source="WORKSPACE",
        existing_cluster_id=CLUSTER,
        notebook_packages=[{"pypi": {"package": "tabulate==0.9.0"}}],
        timeout_seconds=1800,
        poll_interval=10,
    )
