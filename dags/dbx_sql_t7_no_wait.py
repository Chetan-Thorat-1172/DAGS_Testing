from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSQLStatementsOperator

# T7: wait_for_termination=False — fire and forget.
#
# The task should succeed within a couple of seconds, long before the statement
# itself finishes. statement_id must still be published, so a downstream task or
# an operator could follow up on it.
#
# The statement uses sha2-512 over 2e9 rows so it genuinely runs for minutes,
# making the gap between "task green" and "statement finished" unmistakable. It is
# deliberately left running: nothing cancels it, which is the whole point of
# fire-and-forget.
with DAG(
    dag_id="databricks_sql_t7_no_wait",
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
        task_id="fire_and_forget",
        warehouse_id="3e52b555fe3ac722",
        statement="""
            SELECT max(length(sha2(concat_ws('-', a.id, b.id), 512))) AS h
            FROM range(0, 2000000) a
            CROSS JOIN range(0, 1000) b
        """,
        wait_for_termination=False,
    )
