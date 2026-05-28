"""
Test DAG: Task Mapping / .expand() — XCom Reference
"""
from datetime import datetime

try:
    from pi_flow import DAG, PythonOperator, XComArg
except ImportError:
    pass  # Worker import context


def generate_files():
    """Upstream task returns a list — determines how many mapped instances to create."""
    return ["data_2026_01.csv", "data_2026_02.csv", "data_2026_03.csv", "data_2026_04.csv"]


def process_file(filename, prefix="processed_"):
    """Each mapped instance processes one file with the fixed prefix from .partial()."""
    return f"{prefix}{filename}"


try:
    dag = DAG(
        dag_id="test_task_mapping_xcom",
        schedule_interval="@daily",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        description="Tests .expand() with XCom reference from upstream",
    )

    with dag:
        generate_task = PythonOperator(
            task_id="generate_files",
            python_callable=generate_files,
        )

        process_task = PythonOperator(
            task_id="process_file",
            python_callable=process_file,
        ).partial(prefix="out_").expand(filename=XComArg("generate_files"))

        generate_task >> process_task
except NameError:
    pass
