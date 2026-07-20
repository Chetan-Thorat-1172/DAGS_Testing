from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SmtpNotifier

# DAG Fail Alert Not Workinggg
def succeed_task(**kwargs):
    return "i succeeded"


def fail_task(**kwargs):
    raise Exception("Simulated failure for callback test")


with DAG(
    dag_id="test_callbacks",
    schedule_interval=None,
    start_date=datetime(2026, 7, 13),
    catchup=False,
    on_failure_callback=SmtpNotifier(
        to=["Chetan.Thorat@Pibythree.com"],
        subject="DAG test_callbacks FAILED",
    ),
) as dag:

    t_success = PythonOperator(
        task_id="t_success",
        python_callable=succeed_task,
        on_success_callback=SmtpNotifier(
            to=["Chetan.Thorat@Pibythree.com"],
            subject="Task t_success succeeded",
        ),
    )

    t_fail = PythonOperator(
        task_id="t_fail",
        python_callable=fail_task,
        retries=1,
        retry_delay_seconds=5,
        on_failure_callback=SmtpNotifier(
            to=["Chetan.Thorat@Pibythree.com"],
            subject="Task t_fail FAILED",
        ),
        on_retry_callback=SmtpNotifier(
            to=["Chetan.Thorat@Pibythree.com"],
            subject="Task t_fail Retrying",
        ),
    )

    t_success >> t_fail
