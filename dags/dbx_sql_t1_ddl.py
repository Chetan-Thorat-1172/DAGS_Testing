from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSQLStatementsOperator

# T1: DDL. Creates the dedicated test schema, then a table inside it.
#
# create_schema doubles as the setup step for every later test, so nothing has to
# be created by hand. It also exercises the case where catalog/schema are NOT set
# on the task and the statement fully qualifies the name itself.
#
# create_table sets catalog and schema on the task instead, so the unqualified
# table name resolves — proving those two parameters reach the API.
with DAG(
    dag_id="databricks_sql_t1_ddl",
    schedule=None,
    start_date=datetime(2026, 8, 13),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 0,
    },
    tags=["databricks", "sql"],
) as dag:
    create_schema = DatabricksSQLStatementsOperator(
        task_id="create_schema",
        warehouse_id="3e52b555fe3ac722",
        statement="CREATE SCHEMA IF NOT EXISTS maestro_pi.maestro_sql_test",
        poll_interval=5,
    )

    create_table = DatabricksSQLStatementsOperator(
        task_id="create_table",
        warehouse_id="3e52b555fe3ac722",
        # Deliberately unqualified: this only resolves if catalog and schema are
        # applied by the operator.
        statement="""
            CREATE TABLE IF NOT EXISTS events (
                id     BIGINT,
                label  STRING,
                ds     DATE
            )
        """,
        catalog="maestro_pi",
        schema="maestro_sql_test",
        poll_interval=5,
    )

    create_schema >> create_table
