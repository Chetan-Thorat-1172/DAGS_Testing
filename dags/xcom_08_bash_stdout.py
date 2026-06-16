"""xcom_08_bash_stdout.py — Test #8: BashOperator stdout as XCom return_value

WHAT WE'RE TESTING:
  BashOperator captures the command's STDOUT and pushes it as the task's XCom
  return_value. A downstream PythonOperator can then pull and use it.

HOW IT WORKS INTERNALLY:
  1. BashExecutor (executor_bash.go line ~69-70):
     result.Status = StatusSuccess
     result.ReturnValue = stdout.String()   // captured stdout
  2. task_runner.go line ~234: pushes ReturnValue to xcom as "return_value"
  3. Downstream task: ti.xcom_pull(task_ids="bash_producer") returns the stdout string

  NOTE: stdout includes trailing newline from echo. The consumer may need to .strip() it.

HOW TO VERIFY:
  - Both tasks succeed
  - Consumer logs: "Bash produced: hello from bash" (or similar)
  - DB: xcom row for bash_producer has value = "hello from bash\n"
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BashOperator

with DAG(
    dag_id="xcom_08_bash_stdout",
    schedule=None,
    start_date=datetime(2026, 6, 16),
    catchup=False,
    tags=["xcom", "test"],
) as dag:

    bash_producer = BashOperator(
        task_id="bash_producer",
        bash_command="echo hello from bash",
    )

    def read_bash_output(**context):
        ti = context["ti"]
        value = ti.xcom_pull(task_ids="bash_producer", key="return_value")
        cleaned = value.strip() if value else None
        print(f"Bash produced: '{cleaned}'")

        if cleaned != "hello from bash":
            raise ValueError(f"Expected 'hello from bash', got '{cleaned}'")
        return {"received": cleaned}

    consumer = PythonOperator(task_id="consumer", python_callable=read_bash_output)

    bash_producer >> consumer
