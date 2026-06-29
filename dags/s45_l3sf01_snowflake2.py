from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, SQLExecuteQueryOperator

# S45 / L3-SF-01 live proof (Snowflake delegation routing).
# v2: snowflake_default now exists, so this exercises the full
# SQLExecuteQueryOperator -> SQLExecutor -> getConnectionType=='snowflake'
# -> SnowflakeExecutor delegation (executor_sql.go:70-72) that L3-SF-01
# previously made unreachable. (Query auth itself is bounded by this
# account's MFA policy + executor password-only auth = L3-SF-02, separate.)
with DAG(
    dag_id="s45_l3sf01_snowflake2",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 29),
    catchup=False,
    description="L3-SF-01 proof v2: generic SQL op reaches SnowflakeExecutor",
) as dag:
    SQLExecuteQueryOperator(
        task_id="sf_select",
        sql="SELECT 7 AS sf_answer",
        conn_id="snowflake_default",
    )
