from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSQLStatementsOperator

# T5: deliberate failure. EXPECTED TO GO RED — that is the pass condition.
#
# bad_syntax fails at parse in Databricks, which comes back as a rejection on the
# submit call itself (HTTP 200 with a FAILED status and no statement_id). Proves
# the operator surfaces the real reason instead of polling a blank id.
#
# missing_table fails during execution, so it comes back through the poll loop as
# a FAILED state. Proves error_code and message reach the task error.
with DAG(
    dag_id="databricks_sql_t5_failure",
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
        task_id="bad_syntax",
        warehouse_id="3e52b555fe3ac722",
        statement="SELCT 1 FROM",
        poll_interval=5,
    )

    DatabricksSQLStatementsOperator(
        task_id="missing_table",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        statement="SELECT * FROM a_table_that_does_not_exist",
        poll_interval=5,
    )
