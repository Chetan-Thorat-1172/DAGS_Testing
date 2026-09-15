"""Passing values from a manual trigger's conf into Snowflake SQL.

Trigger with a conf such as:

    {"region": "US-WEST", "row_limit": 2, "big": 9007199254740993}

Requires DEMO.PUBLIC.SP_ECHO(MSG STRING, N NUMBER).
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    SQLExecuteQueryOperator,
)

CONN = "snowflake_conn"


def verify(**context):
    """Check each task received the values the run was triggered with."""
    ti = context["ti"]
    conf = context["params"] or {}
    print(f"conf = {conf!r}")

    def row(task_id):
        raw = ti.xcom_pull(task_ids=task_id, key="return_value")
        print(f"{task_id} -> {raw!r}")
        rows = json.loads(raw) if isinstance(raw, str) else raw
        return (rows or [{}])[0]

    region = str(conf.get("region", ""))
    limit = conf.get("row_limit")

    # Templated straight into the SQL text.
    r = row("sql_from_conf")
    assert r.get("REGION") == region, f"sql_from_conf REGION: {r}"

    # Templated into a bind value — the safe form.
    r = row("bind_from_conf")
    assert r.get("REGION") == region, f"bind_from_conf REGION: {r}"

    # Bound into a stored-procedure call.
    r = row("call_echo")
    echoed = list(r.values())[0] if r else ""
    assert region in echoed, f"call_echo: {echoed}"

    # Row count driven by conf.
    rows = json.loads(ti.xcom_pull(task_ids="limit_from_conf", key="return_value"))
    assert len(rows) == int(limit), f"limit_from_conf returned {len(rows)} rows, want {limit}"

    # A large integer must survive the conf -> template -> SQL path exactly.
    r = row("big_int_from_conf")
    assert r.get("BIG") == str(conf.get("big")), (
        f"big integer changed: conf={conf.get('big')} got={r.get('BIG')}"
    )

    print("ALL ASSERTIONS PASSED")
    return "ok"


with DAG(
    dag_id="snowflake_runtime_params",
    description="Uses manual-trigger conf values in Snowflake SQL",
    tags=["livetest", "snowflake"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # Value templated directly into the statement text.
    sql_from_conf = SQLExecuteQueryOperator(
        task_id="sql_from_conf",
        sql="SELECT '{{ .Params.region }}' AS REGION",
    )

    # Same value, templated into a bind instead — no quoting needed.
    bind_from_conf = SQLExecuteQueryOperator(
        task_id="bind_from_conf",
        sql="SELECT :region AS REGION",
        parameters={"region": "{{ .Params.region }}"},
    )

    # Bound into a stored-procedure call.
    call_echo = SQLExecuteQueryOperator(
        task_id="call_echo",
        sql="CALL DEMO.PUBLIC.SP_ECHO(:msg, :n)",
        parameters={"msg": "{{ .Params.region }}", "n": 7},
    )

    # A number from conf shaping the query itself.
    limit_from_conf = SQLExecuteQueryOperator(
        task_id="limit_from_conf",
        sql="SELECT SEQ4() AS N FROM TABLE(GENERATOR(ROWCOUNT => 10)) LIMIT {{ .Params.row_limit }}",
    )

    # A large integer must not be rounded on the way through.
    big_int_from_conf = SQLExecuteQueryOperator(
        task_id="big_int_from_conf",
        sql="SELECT '{{ .Params.big }}' AS BIG",
    )

    check = PythonOperator(
        task_id="verify",
        python_callable=verify,
        provide_context=True,
    )

    [
        sql_from_conf,
        bind_from_conf,
        call_echo,
        limit_from_conf,
        big_int_from_conf,
    ] >> check
