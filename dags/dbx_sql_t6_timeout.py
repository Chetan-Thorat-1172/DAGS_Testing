from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSQLStatementsOperator

# T6: client-side timeout. EXPECTED TO GO RED — that is the pass condition.
#
# The statement hashes ~1e9 rows, which takes minutes on a small warehouse. The
# hash() call is deliberate: a bare count(*) over a cross join can be answered by
# the planner without scanning anything, which would defeat the test.
#
# timeout=20 fires long before the query finishes. What to verify afterwards:
#   - The task fails with "timed out after 20s and was cancelled".
#   - In Databricks query history the statement shows CANCELED, not still running.
#     That is the important half: an abandoned query would keep burning warehouse
#     time with nothing orchestrating it.
with DAG(
    dag_id="databricks_sql_t6_timeout",
    schedule=None,
    start_date=datetime(2026, 8, 13),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 0,
    },
    tags=["databricks", "sql"],
) as dag:
    DatabricksSQLStatementsOperator(
        task_id="slow_query_times_out",
        warehouse_id="3e52b555fe3ac722",
        statement="""
            SELECT max(hash(a.id, b.id)) AS h
            FROM range(0, 1000000) a
            CROSS JOIN range(0, 1000) b
        """,
        timeout=20,
        poll_interval=5,
    )
