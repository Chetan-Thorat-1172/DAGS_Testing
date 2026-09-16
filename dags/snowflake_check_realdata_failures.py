"""Data-quality checks that CATCH a problem, against real Snowflake tables.

Every task fails by design — a red run is correct. These are the shapes that
matter in a pipeline: a volume that moved, a metric that drifted, an invariant
that broke. What is being verified is that the failure message names the actual
value found, so an on-call reader learns what is wrong without re-running the
query by hand.

Checked against metadata.task_instance afterwards:

    SELECT task_id, state, try_number
    FROM metadata.task_instance
    WHERE dag_id = 'snowflake_check_realdata_failures';
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
    dag_id="snowflake_check_realdata_failures",
    description="Data-quality checks catching real problems — every task fails by design",
    tags=["livetest", "snowflake"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # The load is short: 1.5M rows arrived, 2M were expected. A real pipeline
    # would alert on exactly this.
    volume_dropped = SnowflakeValueCheckOperator(
        task_id="volume_dropped",
        sql=f"SELECT COUNT(*) FROM {TPCH}.ORDERS",
        pass_value=2000000,
        tolerance=0.01,
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    # Revenue drifted outside the tolerance the business accepts.
    revenue_drifted = SnowflakeValueCheckOperator(
        task_id="revenue_drifted",
        sql=f"SELECT ROUND(SUM(O_TOTALPRICE)) FROM {TPCH}.ORDERS",
        pass_value=300000000000,
        tolerance=0.02,
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    # An invariant that does not hold: not every customer has placed an order.
    # The check names which of the several conditions broke.
    invariant_broken = SnowflakeCheckOperator(
        task_id="invariant_broken",
        sql=f"""
            SELECT
                COUNT(*) > 0 AS HAS_ROWS,
                COUNT(DISTINCT o.O_CUSTKEY) =
                    (SELECT COUNT(*) FROM {TPCH}.CUSTOMER) AS EVERY_CUSTOMER_ORDERED
            FROM {TPCH}.ORDERS o
        """,
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    # A filter that matches nothing returns no rows at all, which is its own
    # failure — distinct from returning a false value.
    segment_missing = SnowflakeCheckOperator(
        task_id="segment_missing",
        sql=f"""
            SELECT COUNT(*) FROM {TPCH}.CUSTOMER
            WHERE C_MKTSEGMENT = :segment
            HAVING COUNT(*) > 0
        """,
        parameters={"segment": "NO_SUCH_SEGMENT"},
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    # The empty-table case the numeric-text truthiness rule exists for: a
    # COUNT(*) of zero must read as false, not as the non-empty text "0".
    empty_result_is_false = SnowflakeCheckOperator(
        task_id="empty_result_is_false",
        sql=f"SELECT COUNT(*) FROM {TPCH}.CUSTOMER WHERE C_CUSTKEY < 0",
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )
