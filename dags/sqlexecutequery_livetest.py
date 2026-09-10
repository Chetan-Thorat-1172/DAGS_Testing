"""Live end-to-end test for SQLExecuteQueryOperator.

Each task proves one behaviour that was changed or reviewed while the operator
was rebuilt on the shared Snowflake hook. The SQL deliberately references no
tables, schemas or procedures — only scalar expressions and GENERATOR — so it
runs whatever role the connection uses, without needing any grants.

Set CONN below to the Connection ID you created in the UI.
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
    """Read every XCom back and assert the shapes the operator promises."""
    ti = context["ti"]

    def pull(task_id):
        raw = ti.xcom_pull(task_ids=task_id, key="return_value")
        print(f"{task_id} -> {raw!r}")
        return json.loads(raw) if isinstance(raw, str) else raw

    # 1. A single-statement query returns a JSON array of row objects.
    one = pull("select_one")
    assert one == [{"N": "1"}], f"select_one: {one}"

    # 2. Every row comes back, not just the first — the old operator dropped
    #    everything after row 1.
    rows = pull("multi_row")
    assert len(rows) == 3, f"multi_row should return 3 rows, got {rows}"

    # 3. A bound value containing SQL is data, never part of the statement.
    bound = pull("bound_params")
    assert bound[0]["INJECTED"] == "x' OR '1'='1", f"bound_params: {bound}"
    assert bound[0]["BIG"] == "9007199254740993", f"big integer lost precision: {bound}"

    # 4. return_last (the default) keeps only the last statement's rows.
    last = pull("split_statements")
    assert last == [{"B": "2"}], f"split_statements: {last}"

    # 5. return_last=False keeps one entry per statement, boundaries intact.
    allsets = pull("all_results")
    assert allsets == [[{"A": "1"}], [{"B": "2"}]], f"all_results: {allsets}"

    # 6. A per-task warehouse override actually takes effect.
    wh = pull("warehouse_override")
    print(f"warehouse in session: {wh}")
    assert wh[0]["WH"], f"warehouse_override returned no warehouse: {wh}"

    # 7. do_xcom_push=False pushes nothing at all.
    assert pull("no_xcom") is None, "no_xcom should not have pushed an XCom"

    # 8. A query matching no rows pushes nothing.
    assert pull("empty_result") is None, "empty_result should not have pushed an XCom"

    print("ALL ASSERTIONS PASSED")
    return "ok"


with DAG(
    dag_id="sqlexecutequery_livetest",
    description="End-to-end proof of SQLExecuteQueryOperator on the shared Snowflake hook",
    tags=["livetest", "snowflake"],
    start_date=datetime(2026, 1, 1),
    schedule=None,  # manual trigger only
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # Baseline: one statement, one row.
    select_one = SQLExecuteQueryOperator(
        task_id="select_one",
        sql="SELECT 1 AS N",
    )

    # Full result sets: three rows, no table needed.
    multi_row = SQLExecuteQueryOperator(
        task_id="multi_row",
        sql="SELECT SEQ4() AS N FROM TABLE(GENERATOR(ROWCOUNT => 3))",
    )

    # Real bind parameters: the injection-shaped value must come back as data,
    # and the integer must survive past 2^53 exactly.
    bound_params = SQLExecuteQueryOperator(
        task_id="bound_params",
        sql="SELECT :injected AS INJECTED, :big AS BIG",
        parameters={"injected": "x' OR '1'='1", "big": 9007199254740993},
    )

    # Statement splitting; return_last defaults to true.
    split_statements = SQLExecuteQueryOperator(
        task_id="split_statements",
        sql="SELECT 1 AS A; SELECT 2 AS B",
    )

    # Same SQL, every statement's result kept.
    all_results = SQLExecuteQueryOperator(
        task_id="all_results",
        sql="SELECT 1 AS A; SELECT 2 AS B",
        return_last=False,
    )

    # Per-task session override.
    warehouse_override = SQLExecuteQueryOperator(
        task_id="warehouse_override",
        sql="SELECT CURRENT_WAREHOUSE() AS WH, CURRENT_ROLE() AS ROLE",
        warehouse="COMPUTE_WH",
    )

    # Runs, returns nothing.
    no_xcom = SQLExecuteQueryOperator(
        task_id="no_xcom",
        sql="SELECT 1 AS IGNORED",
        do_xcom_push=False,
    )

    # A query that matches nothing pushes no XCom.
    empty_result = SQLExecuteQueryOperator(
        task_id="empty_result",
        sql="SELECT 1 AS N FROM TABLE(GENERATOR(ROWCOUNT => 1)) WHERE 1 = 0",
    )

    check = PythonOperator(
        task_id="verify",
        python_callable=verify,
        provide_context=True,
    )

    [
        select_one,
        multi_row,
        bound_params,
        split_statements,
        all_results,
        warehouse_override,
        no_xcom,
        empty_result,
    ] >> check
