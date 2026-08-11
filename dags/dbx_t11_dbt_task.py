"""T11 — dbt_task with git_source and a SQL warehouse.

Also exercises the validation rule that dbt_task requires git_source.

A dbt task needs compute of its OWN to run the dbt CLI, separate from the
warehouse dbt connects to for SQL. Omitting it fails at submit with:

    400 INVALID_PARAMETER_VALUE: One of job_cluster_key, new_cluster, or
    existing_cluster_id must be specified.

`dbt debug` runs first on purpose: its output reports the profiles directory and
the profile Databricks generated, so if the project's `profile:` name does not
match what Databricks expects, the failure tells us the correct value instead of
us guessing it.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, DatabricksSubmitRunOperator

WAREHOUSE_ID = "3e52b555fe3ac722"
CLUSTER = "0810-102016-bcimmln4"

with DAG(
    dag_id="databricks_t11_dbt_task",
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
        task_id="dbt_run",
        existing_cluster_id=CLUSTER,
        dbt_task={
            "project_directory": "dbt_maestro",
            "commands": ["dbt debug", "dbt run"],
            "warehouse_id": WAREHOUSE_ID,
        },
        git_source={
            "git_url": "https://github.com/Chetan-Thorat-1172/DAGS_Testing",
            "git_provider": "gitHub",
            "git_branch": "local",
        },
        timeout_seconds=3600,
        polling_period_seconds=15,
    )
