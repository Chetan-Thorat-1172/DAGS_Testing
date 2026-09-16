"""Data-quality checks against real Snowflake tables.

Runs against SNOWFLAKE_SAMPLE_DATA.TPCH_SF1, which every Snowflake account has,
so nothing needs to be created. The tables are real and large — ORDERS has 1.5M
rows — so these are the checks you would actually write in a pipeline:
completeness, uniqueness, referential integrity, and expected volumes.

Every check here passes, so a green run is the assertion. The expected values
were read from the data first (see snowflake_probe_context) rather than assumed.

Realistic failures live in snowflake_check_realdata_failures.py.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    SnowflakeCheckOperator,
    SnowflakeValueCheckOperator,
)

CONN = "snowflake_conn"
TPCH = "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1"

with DAG(
    dag_id="snowflake_check_realdata",
    description="Data-quality checks over the TPC-H sample tables",
    tags=["livetest", "snowflake"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # --- Completeness: did the data arrive at all? ---

    customers_present = SnowflakeCheckOperator(
        task_id="customers_present",
        sql=f"SELECT COUNT(*) FROM {TPCH}.CUSTOMER",
    )

    orders_present = SnowflakeCheckOperator(
        task_id="orders_present",
        sql=f"SELECT COUNT(*) FROM {TPCH}.ORDERS",
    )

    # --- Completeness: no missing keys ---
    #
    # Note the `= 0`. A check passes when every value in the row is true, so a
    # "nothing is broken" test has to return a true/false, not the count itself:
    # `SELECT COUNT(*) ... WHERE <bad>` returns 0 when the data is CLEAN, and 0
    # is false — which would fail the check exactly when it should pass.
    no_null_customer_keys = SnowflakeCheckOperator(
        task_id="no_null_customer_keys",
        sql=f"SELECT COUNT(*) = 0 FROM {TPCH}.CUSTOMER WHERE C_CUSTKEY IS NULL",
    )

    no_null_order_dates = SnowflakeCheckOperator(
        task_id="no_null_order_dates",
        sql=f"SELECT COUNT(*) = 0 FROM {TPCH}.ORDERS WHERE O_ORDERDATE IS NULL",
    )

    # --- Uniqueness: the primary key is actually unique ---

    customer_keys_unique = SnowflakeCheckOperator(
        task_id="customer_keys_unique",
        sql=f"SELECT COUNT(*) = COUNT(DISTINCT C_CUSTKEY) FROM {TPCH}.CUSTOMER",
    )

    # --- Referential integrity: every order belongs to a real customer ---

    no_orphan_orders = SnowflakeCheckOperator(
        task_id="no_orphan_orders",
        sql=f"""
            SELECT COUNT(*) = 0
            FROM {TPCH}.ORDERS o
            LEFT JOIN {TPCH}.CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
            WHERE c.C_CUSTKEY IS NULL
        """,
    )

    # --- Several conditions in one check ---
    #
    # Every value in the row must be true, so one task can assert a whole set of
    # invariants and name the one that broke.
    order_invariants = SnowflakeCheckOperator(
        task_id="order_invariants",
        sql=f"""
            SELECT
                COUNT(*) > 0                        AS HAS_ROWS,
                COUNT(*) = COUNT(DISTINCT O_ORDERKEY) AS KEYS_UNIQUE,
                MIN(O_TOTALPRICE) > 0               AS PRICES_POSITIVE,
                COUNT_IF(O_ORDERSTATUS IS NULL) = 0 AS STATUS_COMPLETE
            FROM {TPCH}.ORDERS
        """,
    )

    # --- Bind parameters: check a slice of the data ---

    building_segment_populated = SnowflakeCheckOperator(
        task_id="building_segment_populated",
        sql=f"SELECT COUNT(*) FROM {TPCH}.CUSTOMER WHERE C_MKTSEGMENT = :segment",
        parameters={"segment": "BUILDING"},
    )

    # --- Session overrides: run against a database and schema per task ---
    #
    # With these set, the SQL needs no qualifying — useful when the same check
    # runs against several environments.
    unqualified_via_override = SnowflakeCheckOperator(
        task_id="unqualified_via_override",
        sql="SELECT COUNT(*) FROM CUSTOMER",
        database="SNOWFLAKE_SAMPLE_DATA",
        schema="TPCH_SF1",
    )

    # --- Expected volumes: the row count is exactly what it should be ---

    region_count = SnowflakeValueCheckOperator(
        task_id="region_count",
        sql=f"SELECT COUNT(*) FROM {TPCH}.REGION",
        pass_value=5,
    )

    nation_count = SnowflakeValueCheckOperator(
        task_id="nation_count",
        sql=f"SELECT COUNT(*) FROM {TPCH}.NATION",
        pass_value=25,
    )

    customer_count = SnowflakeValueCheckOperator(
        task_id="customer_count",
        sql=f"SELECT COUNT(*) FROM {TPCH}.CUSTOMER",
        pass_value=150000,
    )

    # --- Expected volumes, with room to move ---
    #
    # A daily load will not land on an exact number, so tolerance is the usual
    # shape: this passes for any count within 1% of 1.5M.
    order_volume = SnowflakeValueCheckOperator(
        task_id="order_volume",
        sql=f"SELECT COUNT(*) FROM {TPCH}.ORDERS",
        pass_value=1500000,
        tolerance=0.01,
    )

    # Revenue is the metric most worth alerting on, and the one least likely to
    # be exact. 2% either side of the known total.
    total_revenue = SnowflakeValueCheckOperator(
        task_id="total_revenue",
        sql=f"SELECT ROUND(SUM(O_TOTALPRICE)) FROM {TPCH}.ORDERS",
        pass_value=226829306447,
        tolerance=0.02,
    )
