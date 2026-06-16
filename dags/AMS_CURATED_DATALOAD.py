from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator

with DAG(
    dag_id="AMS_CURATED_DATALOAD",
    schedule_interval=None,
    start_date=datetime(2026, 6, 12),
    catchup=False,
    default_args={"snowflake_conn_id": "Ram_SF_Conn", "retries": 1, "retry_delay_seconds": 5},
    description="AMS Daily Raw to Curated Data Load",
) as dag:

    t01 = SnowflakeOperator(task_id="TS_EXECUTE_AMS_DBT_PR", sql="EXECUTE DBT PROJECT AMS_POC.CMS.AMS ARGS = 'run';")

    # Dependencies:  t01
    t01
