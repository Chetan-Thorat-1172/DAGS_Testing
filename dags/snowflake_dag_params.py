"""DAG-level typed params driving a Snowflake query.

Trigger manually and the UI renders a form from these params. Identifiers
(database, schema, table) are templated into the SQL text — they cannot be bind
parameters — while the region filter is bound as a value.

Requires DEMO.PUBLIC.CUSTOMERS(ID NUMBER, NAME STRING, REGION STRING).
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    SQLExecuteQueryOperator,
)
from dag_parser.dynamic.params import Param

CONN = "snowflake_conn"

TARGET = "{{ .Params.target_db }}.{{ .Params.target_schema }}.{{ .Params.target_table }}"


def verify(**context):
    """Check every task used the values the run was triggered with."""
    ti = context["ti"]
    p = context["params"] or {}
    print(f"params = {p!r}")

    def rows(task_id):
        raw = ti.xcom_pull(task_ids=task_id, key="return_value")
        print(f"{task_id} -> {raw!r}")
        return json.loads(raw) if isinstance(raw, str) else raw

    # Identifiers templated into the statement resolved to a real table.
    total = int(rows("count_rows")[0]["N"])
    assert total > 0, f"count_rows returned {total}"

    # row_limit shaped the result.
    sample = rows("sample_rows")
    assert len(sample) == int(p["row_limit"]), f"sample_rows: {len(sample)} rows"

    # Identifiers templated, the filter value bound.
    filtered = rows("filter_by_region")
    assert filtered, "filter_by_region returned nothing"
    assert all(r["REGION"] == p["region"] for r in filtered), f"filter_by_region: {filtered}"

    # Same table reached through per-task session overrides, themselves
    # templated from the params.
    via_override = int(rows("count_via_override")[0]["N"])
    assert via_override == total, f"override count {via_override} != {total}"

    print("ALL ASSERTIONS PASSED")
    return "ok"


with DAG(
    dag_id="snowflake_dag_params",
    description="DAG-level typed params supplying the Snowflake target at trigger time",
    tags=["livetest", "snowflake"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
    params={
        "target_db": Param("string", default="DEMO", title="Target database"),
        "target_schema": Param("string", default="PUBLIC", title="Target schema"),
        "target_table": Param("string", default="CUSTOMERS", title="Target table"),
        "row_limit": Param("integer", default=2, minimum=1, maximum=100,
                           title="Sample rows"),
        # An enum constrains a value that reaches the SQL, so the form cannot
        # supply something unexpected.
        "region": Param("string", default="US-WEST",
                        enum=["US-WEST", "US-EAST", "EU", "APAC"],
                        title="Region filter"),
    },
) as dag:

    # Identifiers must be templated — a bind parameter is a value, not a name.
    count_rows = SQLExecuteQueryOperator(
        task_id="count_rows",
        sql=f"SELECT COUNT(*) AS N FROM {TARGET}",
    )

    sample_rows = SQLExecuteQueryOperator(
        task_id="sample_rows",
        sql=f"SELECT * FROM {TARGET} LIMIT {{{{ .Params.row_limit }}}}",
    )

    # Identifiers templated, the filter value bound.
    filter_by_region = SQLExecuteQueryOperator(
        task_id="filter_by_region",
        sql=f"SELECT ID, NAME, REGION FROM {TARGET} WHERE REGION = :region",
        parameters={"region": "{{ .Params.region }}"},
    )

    # The session overrides are templated too, so the table needs no qualifying.
    count_via_override = SQLExecuteQueryOperator(
        task_id="count_via_override",
        sql="SELECT COUNT(*) AS N FROM {{ .Params.target_table }}",
        database="{{ .Params.target_db }}",
        schema="{{ .Params.target_schema }}",
    )

    check = PythonOperator(
        task_id="verify",
        python_callable=verify,
        provide_context=True,
    )

    [count_rows, sample_rows, filter_by_region, count_via_override] >> check
