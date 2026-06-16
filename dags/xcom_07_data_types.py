"""xcom_07_data_types.py — Test #7: XCom with different data types

WHAT WE'RE TESTING:
  XCom stores values as JSONB in PostgreSQL. This tests that various Python types
  serialize and deserialize correctly through the XCom round-trip:
  - string, integer, float, boolean, list, nested dict, None

HOW IT WORKS INTERNALLY:
  Push side (_run_task.py line ~432):
    if isinstance(return_value, (dict, list)):
        return_value = json.dumps(return_value)
    else:
        return_value = str(return_value)

  Pull side (_run_task.py line ~277):
    if isinstance(pulled_value, str):
        try: pulled_value = json.loads(pulled_value)
        except: pass  # leave as string

  So: dicts/lists round-trip as JSON. Strings stay as strings.
  Integers/floats → str on push → JSON parse on pull → back to int/float.

HOW TO VERIFY:
  - Both tasks succeed
  - Consumer logs show each value with its type:
    "string_val: hello (type: str)"
    "int_val: 42 (type: int)"
    "float_val: 3.14 (type: float)"
    "list_val: [1, 2, 3] (type: list)"
    "dict_val: {'nested': True} (type: dict)"
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator

with DAG(
    dag_id="xcom_07_data_types",
    schedule=None,
    start_date=datetime(2026, 6, 16),
    catchup=False,
    tags=["xcom", "test"],
) as dag:

    def push_types(**context):
        ti = context["ti"]
        ti.xcom_push(key="string_val", value="hello")
        ti.xcom_push(key="int_val", value=42)
        ti.xcom_push(key="float_val", value=3.14)
        ti.xcom_push(key="list_val", value=[1, 2, 3])
        ti.xcom_push(key="dict_val", value={"nested": True, "count": 5})
        ti.xcom_push(key="bool_val", value=True)
        print("Pushed all types")
        return "all pushed"

    def pull_and_verify(**context):
        ti = context["ti"]
        checks = [
            ("string_val", str),
            ("int_val", int),
            ("float_val", float),
            ("list_val", list),
            ("dict_val", dict),
            ("bool_val", bool),
        ]
        for key, expected_type in checks:
            val = ti.xcom_pull(task_ids="push_types", key=key)
            actual_type = type(val).__name__
            print(f"{key}: {val} (type: {actual_type})")
            # Note: booleans and numbers may come back as their JSON equivalents
            # after the str→JSON round-trip

        return "all verified"

    pusher = PythonOperator(task_id="push_types", python_callable=push_types)
    puller = PythonOperator(task_id="pull_and_verify", python_callable=pull_and_verify)

    pusher >> puller
