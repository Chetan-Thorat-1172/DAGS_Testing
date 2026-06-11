from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def marker(label, **context):
    print(f"[AMS_CMS_RAW_DATALOAD] {label} executed at {datetime.now()}")


with DAG(
    dag_id="AMS_CMS_RAW_DATALOAD",
    schedule_interval=None,
    start_date=datetime(2026, 6, 11),
    catchup=False,
    default_args={"snowflake_conn_id": "svc_pi_flow_conn", "retries": 1, "retry_delay_seconds": 5},
    description="AMS logistics CMS raw data ingestion - Move Count, Employee Hours, HHG Claims, Hire Date",
) as dag:

    t01 = PythonOperator(task_id="cms_start", python_callable=lambda **c: marker("cms_start", **c))
    t02 = SnowflakeOperator(task_id="load_move_count", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('ams_cms_load_move_count');")
    t03 = SnowflakeOperator(task_id="load_employee_hours", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('ams_cms_load_employee_hours');")
    t04 = SnowflakeOperator(task_id="load_hhg_claims", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('ams_cms_load_hhg_claims');")
    t05 = SnowflakeOperator(task_id="load_hire_date_tenure", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('ams_cms_load_hire_date_tenure');")
    t06 = SnowflakeOperator(task_id="validate_cms_raw", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('ams_cms_validate_raw');")
    t07 = SnowflakeOperator(task_id="merge_cms_to_raw", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('ams_cms_merge_to_raw');")
    t08 = SnowflakeOperator(task_id="audit_cms_load", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('ams_cms_audit_load');")
    t09 = PythonOperator(task_id="cms_end", python_callable=lambda **c: marker("cms_end", **c), trigger_rule="all_done")

    # Dependencies: start -> parallel loads -> validate -> merge -> audit -> end
    t01 >> [t02, t03, t04, t05]
    [t02, t03, t04, t05] >> t06
    t06 >> t07 >> t08 >> t09
