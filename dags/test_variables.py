from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BashOperator


def read_variables(**kwargs):
    var = kwargs.get("var", {})
    env = var.get("env_name", "NOT_FOUND")
  
    print(f"env_name={env}", flush=True)

    return f"env={env}"

with DAG(
    dag_id="test_variables",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:

    # Python access via kwargs["var"]
    t_python = PythonOperator(
        task_id="t_read_vars",
        python_callable=read_variables,
    )

    # Go template access via {{ .Var.key }}
    t_template = BashOperator(
        task_id="t_template_vars",
        bash_command="echo 'env={{ .Var.env_name }}' ",
    )
