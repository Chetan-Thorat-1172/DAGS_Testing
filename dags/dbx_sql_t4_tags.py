from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSQLStatementsOperator

# T4: query tags.
#
# custom_tags adds two author tags alongside the built-in maestro_* identity tags.
# After this runs, Databricks query history should show BOTH sets on the statement.
#
# tags_disabled turns the identity tags off, leaving only the author's — proving
# include_metadata_query_tags actually reaches the payload.
#
# collision proves an author tag wins over a built-in one with the same key.
with DAG(
    dag_id="databricks_sql_t4_tags",
    schedule=None,
    start_date=datetime(2026, 8, 13),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 0,
    },
    tags=["databricks", "sql"],
) as dag:
    custom_tags = DatabricksSQLStatementsOperator(
        task_id="custom_tags",
        warehouse_id="3e52b555fe3ac722",
        statement="SELECT 'tagged' AS marker",
        query_tags={"team": "analytics", "cost_center": "cc-42"},
        poll_interval=5,
    )

    tags_disabled = DatabricksSQLStatementsOperator(
        task_id="metadata_tags_disabled",
        warehouse_id="3e52b555fe3ac722",
        statement="SELECT 'untagged' AS marker",
        query_tags={"team": "analytics"},
        include_metadata_query_tags=False,
        poll_interval=5,
    )

    collision = DatabricksSQLStatementsOperator(
        task_id="author_overrides_builtin",
        warehouse_id="3e52b555fe3ac722",
        statement="SELECT 'override' AS marker",
        query_tags={"maestro_dag_id": "overridden_by_author"},
        poll_interval=5,
    )

    custom_tags >> tags_disabled >> collision
