from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSQLStatementsOperator

# T0: the cheapest possible statement.
# Needs only CAN USE on the SQL warehouse — no catalog, schema or table
# privileges. If this passes, the whole path works: connection resolution,
# Statement Execution API submit, poll, terminal state.
with DAG(
    dag_id="databricks_sql_t0_smoke",
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
        task_id="select_one",
        warehouse_id="3e52b555fe3ac722",
        statement="SELECT 1",
        poll_interval=5,
    )
