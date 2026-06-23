from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime


def w():
    return 1


# #4 residual (S33, bakes into the py8 image): a non-JSON-serializable op_kwarg must
# fail the DAG at PARSE time with a CLEAN, NAMED error (task + key + type), not an
# opaque traceback. The value {1,2,3} is a set -> not JSON-serializable.
with DAG(
    dag_id="s34_py8_bad",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    PythonOperator(task_id="bad", python_callable=w, op_kwargs={"x": {1, 2, 3}})
