"""
Session 32 LIVE probe DAG: Python operator #7 (return-value JSON typing +
do_xcom_push) and #9 / L3-PY-01 (mapped per-index xcom + loud-failure).

Trigger manually (psql seed of a queued manual dag_run).

Proofs (read persisted XCOM JSONB via psql, not happy-path logs):
  ret_int   return 5     -> XCOM VALUE = 5     (number, not "5")
  ret_bool  return True  -> XCOM VALUE = true  (boolean, not "True")
  ret_str   return "5"   -> XCOM VALUE = "5"   (JSON string, distinct from int 5)
  no_push   return 5 with do_xcom_push=False -> NO return_value XCOM row
  mapped_push .expand() over [10,20,30]; each index ti.xcom_push(key=sq, value=n*n)
              -> three rows at MAP_INDEX 0/1/2 = 100/400/900 (not one clobbered -1)
  collector ti.xcom_pull(task_ids=mapped_push, key=sq, map_indexes="all")
              -> [100,400,900] (deterministic, map-index aware)
  loud_fail repoints ti to a dead port then xcom_pull -> RAISES -> task 'failed'
              (pre-#9 it silently returned None and the task 'succeeded')
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, XComArg


def ret_int(**context):
    return 5


def ret_bool(**context):
    return True


def ret_str(**context):
    return "5"


def gen(**context):
    return [10, 20, 30]


def mapped_push(n, **context):
    # Explicit per-index push: each mapped instance writes its own map_index.
    context["ti"].xcom_push(key="sq", value=n * n)
    return n


def collector(**context):
    vals = context["ti"].xcom_pull(task_ids="mapped_push", key="sq", map_indexes="all")
    return vals


def loud_fail(**context):
    # Force an internal-API transport failure: repoint at a dead port. The shim
    # retries then RAISES, so the task must fail loudly (not silently return None).
    ti = context["ti"]
    ti._api_url = "http://127.0.0.1:1"
    return ti.xcom_pull(task_ids="ret_int", key="return_value")


with DAG(
    dag_id="s32_py79_probe",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="S32 #7 return typing + #9 mapped xcom/map_index + loud-failure",
    tags=["regression", "cat3", "python", "s32"],
) as dag:
    t_int = PythonOperator(task_id="ret_int", python_callable=ret_int)
    t_bool = PythonOperator(task_id="ret_bool", python_callable=ret_bool)
    t_str = PythonOperator(task_id="ret_str", python_callable=ret_str)
    t_nopush = PythonOperator(task_id="no_push", python_callable=ret_int, do_xcom_push=False)

    t_gen = PythonOperator(task_id="gen", python_callable=gen)
    t_mapped = PythonOperator(task_id="mapped_push", python_callable=mapped_push).expand(
        n=XComArg("gen")
    )
    t_collect = PythonOperator(task_id="collector", python_callable=collector)

    t_loud = PythonOperator(task_id="loud_fail", python_callable=loud_fail)

    t_gen >> t_mapped >> t_collect
