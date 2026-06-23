"""
S33 depth probe: Python operator #5 (execution-context Bucket A) + #10 (binder).

#5 ctx_probe: a **kwargs callable reads ts/ts_nodash/ts_nodash_with_tz and
   macros.ds_add(ds,7); it also confirms data_interval_start raises KeyError
   (intentionally OMITTED -- loud, not silently-wrong). Returns a dict XCom.

#10 binding (the cases that demonstrate the fix -- NO op_kwargs):
   bind_named def f(ds): pre-fix got the WHOLE context dict positionally; post-fix
     gets context["ds"] (a str) by name-intersection.
   bind_two  def g(ti, ds): pre-fix raised TypeError (1 positional for 2 params);
     post-fix binds both by name.
   bind_kwargs def h(**kwargs): gets the full context.
   bind_ctx  def c(context): narrow back-compat -- still gets the whole dict.

Seed a manual queued run; read each return_value XCom JSONB via psql.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ctx_probe(**kwargs):
    out = {
        "ds": kwargs["ds"],
        "ts": kwargs["ts"],
        "ts_nodash": kwargs["ts_nodash"],
        "ts_nodash_with_tz": kwargs["ts_nodash_with_tz"],
        "ds_plus7": kwargs["macros"].ds_add(kwargs["ds"], 7),
        "ds_fmt": kwargs["macros"].ds_format(kwargs["ds"], "%Y-%m-%d", "%Y%m%d"),
        "macros_dt_year": kwargs["macros"].datetime(2026, 1, 1).year,
    }
    try:
        _ = kwargs["data_interval_start"]
        out["interval_present"] = True
    except KeyError:
        out["interval_keyerror"] = True
    return out


def bind_named(ds):
    # pre-fix: ds == whole context dict; post-fix: ds == context["ds"] (str)
    return {"ds_type": type(ds).__name__, "ds_val": str(ds)[:32]}


def bind_two(ti, ds):
    # pre-fix: TypeError (task failed); post-fix: both bound by name
    return {"ti_has_xcom_pull": hasattr(ti, "xcom_pull"), "ds_type": type(ds).__name__, "ds_val": ds}


def bind_kwargs(**kwargs):
    return {
        "n_keys": len(kwargs),
        "got_ds": "ds" in kwargs,
        "got_ti": "ti" in kwargs,
        "got_macros": "macros" in kwargs,
    }


def bind_ctx(context):
    # narrow back-compat: sole param named `context` -> whole dict
    return {"is_dict": isinstance(context, dict), "has_ds": isinstance(context, dict) and "ds" in context}


with DAG(
    dag_id="s33_py510_probe",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="S33 #5 context Bucket A + #10 binder depth probe",
    tags=["regression", "cat3", "python", "s33"],
) as dag:
    PythonOperator(task_id="ctx_probe", python_callable=ctx_probe)
    PythonOperator(task_id="bind_named", python_callable=bind_named)
    PythonOperator(task_id="bind_two", python_callable=bind_two)
    PythonOperator(task_id="bind_kwargs", python_callable=bind_kwargs)
    PythonOperator(task_id="bind_ctx", python_callable=bind_ctx)
