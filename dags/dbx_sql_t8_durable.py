from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSQLStatementsOperator

# T8: durable reattach — the Codex fix, tested live.
#
# The statement must still be running when the worker is killed, so it uses
# sha2-512 over 2e9 rows: heavy enough to resist Photon's vectorisation and run for
# minutes. (An earlier version used hash() and finished in 2.5 seconds, which left
# no window to restart into.)
#
# With retries=2 and durable=True (the default), attempt 2 must RECONNECT to the
# statement attempt 1 started rather than submitting a second one.
#
# Procedure:
#   1. Trigger this DAG.
#   2. Wait for the databricks_statement_id XCom to appear, and note it.
#   3. docker restart maestro-pi-local-gocore
#   4. Wait for the task to finish.
#
# Pass conditions:
#   - The task ends green on attempt 2.
#   - databricks_statement_id is UNCHANGED between attempts.
#   - Databricks query history shows ONE statement for this task, not two.
#     Two would mean the same multi-billion-row scan was paid for twice, which is
#     exactly what durable exists to prevent.
with DAG(
    dag_id="databricks_sql_t8_durable",
    schedule=None,
    start_date=datetime(2026, 8, 13),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 2,
        "retry_delay_seconds": 5,
    },
    tags=["databricks", "sql"],
) as dag:
    DatabricksSQLStatementsOperator(
        task_id="durable_statement",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        # Writes its result, so a duplicate run would be observable in the table
        # as well as in query history.
        statement="""
            CREATE OR REPLACE TABLE durable_probe AS
            SELECT max(length(sha2(concat_ws('-', a.id, b.id), 512))) AS h
            FROM range(0, 2000000) a
            CROSS JOIN range(0, 1000) b
        """,
        durable=True,
        poll_interval=5,
    )
