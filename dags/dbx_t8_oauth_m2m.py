"""T8 — OAuth machine-to-machine authentication.

Identical workload to T1; the only difference is the connection, which uses a
service principal's OAuth client credentials instead of a Personal Access Token.
A success proves the token mint at {host}/oidc/v1/token, the caching path, and
that the minted token is accepted by the Jobs API.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, DatabricksSubmitRunOperator

WORKSPACE = "/Workspace/Users/ovaizsce121@siesgst.ac.in"
CLUSTER = "0810-102016-bcimmln4"

with DAG(
    dag_id="databricks_t8_oauth_m2m",
    schedule=None,
    start_date=datetime(2026, 8, 11),
    catchup=False,
    default_args={
        # The OAuth connection created in the Connections UI.
        "connection_id": "databricks_oauth",
        "retries": 0,
    },
    tags=["databricks"],
) as dag:
    DatabricksSubmitRunOperator(
        task_id="notebook_via_oauth",
        existing_cluster_id=CLUSTER,
        notebook_task={
            "notebook_path": f"{WORKSPACE}/maestro_smoke",
            "base_parameters": {
                "ds": "{{ .DS }}",
            },
        },
        timeout_seconds=1200,
        polling_period_seconds=10,
    )
