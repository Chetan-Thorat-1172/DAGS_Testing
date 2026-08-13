from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSQLStatementsOperator

# T7: wait_for_termination=False — fire and forget.
#
# The task should succeed within a couple of seconds, long before the statement
# itself finishes. statement_id must still be published, so a downstream task or
# an operator could follow up on it.
#
# The statement is slow on purpose, so the gap between "task green" and "statement
# finished" is unmistakable.
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
            SELECT max(hash(a.id, b.id)) AS h
            FROM range(0, 500000) a
            CROSS JOIN range(0, 500) b
        """,
        wait_for_termination=False,
    )
