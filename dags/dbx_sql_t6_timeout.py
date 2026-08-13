from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSQLStatementsOperator

# T6: client-side timeout. EXPECTED TO GO RED — that is the pass condition.
#
# Making a query reliably slow on a Photon warehouse takes effort: an earlier
# attempt hashed ~1e9 rows and finished in 2.5 seconds. sha2-512 per row is far
# heavier than hash() and resists vectorisation, so 2e9 rows of it takes minutes.
#
# timeout=20 fires well before that. What to verify afterwards:
#   - The task fails with "timed out after 20s and was cancelled".
#   - In Databricks query history the statement shows CANCELED, not still running.
#     That is the important half: an abandoned query would keep burning warehouse
#     time with nothing orchestrating it.
#
# The bound is checked on each poll tick, so with poll_interval=5 it fires between
# 20 and 25 seconds rather than exactly on 20.
SLOW_STATEMENT = """
    SELECT max(length(sha2(concat_ws('-', a.id, b.id), 512))) AS h
    FROM range(0, 2000000) a
    CROSS JOIN range(0, 1000) b
"""

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
        statement=SLOW_STATEMENT,
        timeout=20,
        poll_interval=5,
    )
