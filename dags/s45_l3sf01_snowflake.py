from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, SQLExecuteQueryOperator

# S45 / L3-SF-01 live proof (Snowflake delegation path).
# Uses the GENERIC SQLExecuteQueryOperator (the class that crashed at parse
# before the fix) pointed at a snowflake-type connection. This exercises the
# executor_sql.go getConnectionType -> SnowflakeExecutor delegation (the path
# the gap analysis flagged as unreachable), proving the whole generic family
# now reaches execution.
with DAG(
    dag_id="s45_l3sf01_snowflake",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 29),
    catchup=False,
    description="L3-SF-01 proof: generic SQLExecuteQueryOperator -> SnowflakeExecutor delegation",
) as dag:
    SQLExecuteQueryOperator(
        task_id="sf_select",
        sql="SELECT 7 AS sf_answer",
        conn_id="snowflake_default",
    )
