from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, SnowflakeOperator

with DAG(
    dag_id="test_snowflake_exec",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 25),
    catchup=False,
) as dag:
    # Simple SELECT — result goes to XCom
    t_select = SnowflakeOperator(
        task_id="t_select",
        sql="SELECT CURRENT_TIMESTAMP() AS ts, CURRENT_USER() AS usr",
        snowflake_conn_id="snowflake_default",
    )

    # DDL — no result set, just succeeds
    t_ddl = SnowflakeOperator(
        task_id="t_ddl",
        sql="CREATE OR REPLACE TEMP TABLE piflow_test_tbl (id INT, name VARCHAR)",
        snowflake_conn_id="snowflake_default",
    )

    # Insert + Select to verify
    t_insert = SnowflakeOperator(
        task_id="t_insert",
        sql="INSERT INTO piflow_test_tbl VALUES (1, 'hello'), (2, 'world')",
        snowflake_conn_id="snowflake_default",
    )

    # Query the inserted data — first row to XCom
    t_query = SnowflakeOperator(
        task_id="t_query",
        sql="SELECT * FROM piflow_test_tbl ORDER BY id LIMIT 1",
        snowflake_conn_id="snowflake_default",
    )

    t_ddl >> t_insert >> t_query
