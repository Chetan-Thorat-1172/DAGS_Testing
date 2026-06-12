from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator
 
with DAG(
    dag_id="AMS_CMS_RAW_DATALOAD",
    schedule_interval=None,
    start_date=datetime(2026, 6, 12),
    catchup=False,
    default_args={"snowflake_conn_id": "svc_pi_flow_conn", "retries": 1, "retry_delay_seconds": 5},
    description="AMS logistics CMS raw data ingestion - Move Count, Employee Hours, HHG Claims, Hire Date",
) as dag:

    t01 = SnowflakeOperator(task_id="TS_LOAD_CMS_DATA", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('ams_cms_load_move_count');")

    # Dependencies: t01
    t01
