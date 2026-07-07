from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, EmailOperator


with DAG(
    dag_id="test_email_operator",
    schedule_interval=None,
    start_date=datetime(2026, 7, 7),
    catchup=False,
) as dag:

    send_email = EmailOperator(
        task_id="send_alert",
        to=["Chetan.Thorat@Pibythree.com"],
        subject="PI-FLOW Email Test",
        html_content=(
            "<h2>Hello from PI-FLOW!</h2><p>This is a test email sent by EmailOperator.</p>"
        ),
    )
