"""T7 — libraries installed for the run.

The notebook imports `tabulate` and returns its version, so a success proves the
library was actually installed for this run rather than the payload merely being
accepted.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, DatabricksSubmitRunOperator

WORKSPACE = "/Workspace/Users/ovaizsce121@siesgst.ac.in"
CLUSTER = "0810-102016-bcimmln4"

with DAG(
    dag_id="databricks_t7_libraries",
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
        task_id="notebook_with_pypi_library",
        existing_cluster_id=CLUSTER,
        notebook_task={
            "notebook_path": f"{WORKSPACE}/maestro_lib_check",
        },
        libraries=[{"pypi": {"package": "tabulate==0.9.0"}}],
        timeout_seconds=1800,
        polling_period_seconds=10,
    )
