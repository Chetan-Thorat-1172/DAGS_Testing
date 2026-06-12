from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator

with DAG(
    dag_id="AMS_CURATED_DATALOAD",
    schedule_interval=None,
    start_date=datetime(2026, 6, 12),
    catchup=False,
    default_args={"snowflake_conn_id": "svc_pi_flow_conn", "retries": 1, "retry_delay_seconds": 5},
    description="AMS Daily Raw to Curated Data Load",
) as dag:

    t01 = SnowflakeOperator(task_id="TS_EXECUTE_AMS_DBT_PR", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.AMS_DBT_PROJECT ARGS = 'run';")

    # Dependencies:
    t01
