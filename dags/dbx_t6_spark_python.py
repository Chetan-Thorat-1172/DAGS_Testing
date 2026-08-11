"""T6 — spark_python_task against a Workspace .py file.

Proves the non-notebook task shape works and that `parameters` are passed and
templated. A Python file task produces no notebook exit value, so this task must
succeed with NO `return_value` XCom — that absence is the expected result, not a
failure.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, DatabricksSubmitRunOperator

WORKSPACE = "/Workspace/Users/ovaizsce121@siesgst.ac.in"
CLUSTER = "0810-102016-bcimmln4"

with DAG(
    dag_id="databricks_t6_spark_python",
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
        task_id="python_file",
        existing_cluster_id=CLUSTER,
        spark_python_task={
            "python_file": f"{WORKSPACE}/maestro_spark_job.py",
            "parameters": ["--ds", "{{ .DS }}", "--label", "t6"],
        },
        timeout_seconds=1200,
        polling_period_seconds=10,
    )
