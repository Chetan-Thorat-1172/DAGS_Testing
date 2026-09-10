"""
piqlens_daily_email_report
---------------------------
Calls the existing SEND_PIQLENS_HTML_REPORT stored procedure, which already
does the full job internally: fetch from PIQLENS_REPORT_VIEW, build the
HTML report (with KPI cards), and send it via SYSTEM$SEND_EMAIL.

Manually triggered only (schedule=None) while getting comfortable with PI-Flow.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, SnowflakeOperator

RECIPIENT_EMAIL = "bharat.rao@pibythree.com"  # <-- replace with real recipient

with DAG(
    dag_id="email_notification_dag",
    description="Calls SEND_PIQLENS_HTML_REPORT to email the PIQLens daily validation report",
    tags=["JGW-HEC-Engg"],
    start_date=datetime(2026, 1, 1),
    schedule=None,  # manual trigger only, while learning PI-Flow
) as dag:

    bound_params = SQLExecuteQueryOperator(
        task_id="send_piqlens_report",
        snowflake_conn_id="snowflake_conn",
        sql=f"""
            CALL PIQLENS_DQ_DB.PIQLENS_DQ.SEND_PIQLENS_HTML_REPORT('{RECIPIENT_EMAIL}');
        """,
    )
    


