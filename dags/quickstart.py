from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BashOperator

with DAG(
    dag_id="hello_piflow",
    schedule="@daily",
    start_date=datetime(2026, 6, 15),
    catchup=False,
    tags=["tutorial"],
) as dag:

    def say_hello(**context):
        print("Hello from", context["ds"])
        return {"greeted": True}

    hello = PythonOperator(task_id="hello", python_callable=say_hello)
    done = BashOperator(task_id="done", bash_command="echo finished")
