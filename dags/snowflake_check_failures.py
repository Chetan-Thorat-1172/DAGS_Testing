"""Live test for how a FAILING Snowflake check behaves.

Every task here is expected to FAIL — a red run is the correct outcome. What is
being verified is not that they fail but HOW:

  * a failed verdict with retry_on_failure=False must NOT consume its retries
    (try_number stays 1), because the data cannot change between attempts;
  * a failed verdict with retry_on_failure left at its default MUST retry;
  * a failure that is not a verdict — a query error, or a value that cannot be
    converted to a number — must retry normally EVEN when retry_on_failure is
    False, because a retry might genuinely get past it.

After the run, check the outcome against metadata.task_instance:

    SELECT task_id, state, try_number
    FROM metadata.task_instance
    WHERE dag_id = 'snowflake_check_failures'
    ORDER BY task_id;

Expected: every row failed, with try_number as named in each task below.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    SnowflakeCheckOperator,
    SnowflakeValueCheckOperator,
)

CONN = "snowflake_conn"

with DAG(
    dag_id="snowflake_check_failures",
    description="How a failing Snowflake check behaves — every task fails by design",
    tags=["livetest", "snowflake"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # --- A failed verdict, retries switched off: must not retry (try 1) ---

    fail_falsy_terminal = SnowflakeCheckOperator(
        task_id="fail_falsy_terminal",
        sql="SELECT 0",
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    fail_zero_rows_terminal = SnowflakeCheckOperator(
        task_id="fail_zero_rows_terminal",
        sql="SELECT 1 WHERE 1 = 0",
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    fail_value_mismatch_terminal = SnowflakeValueCheckOperator(
        task_id="fail_value_mismatch_terminal",
        sql="SELECT 5",
        pass_value=10,
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    # --- A failed verdict, retries left on: must retry (try 2) ---

    fail_falsy_retrying = SnowflakeCheckOperator(
        task_id="fail_falsy_retrying",
        sql="SELECT 0",
        retries=1,
        retry_delay_seconds=5,
    )

    # --- Not a verdict: must retry even with retry_on_failure=False (try 2) ---

    # 'abc' cannot become a number, so this is a mismatch between the query and
    # the check rather than a judgement on the data. The reference raises a
    # plain exception here, not the one retry_on_failure governs.
    fail_conversion_retries = SnowflakeValueCheckOperator(
        task_id="fail_conversion_retries",
        sql="SELECT 'abc'",
        pass_value=10,
        retry_on_failure=False,
        retries=1,
        retry_delay_seconds=5,
    )

    # A broken query is likewise not a verdict.
    fail_bad_sql_retries = SnowflakeCheckOperator(
        task_id="fail_bad_sql_retries",
        sql="SELECT * FROM A_TABLE_THAT_DOES_NOT_EXIST_XYZ",
        retry_on_failure=False,
        retries=1,
        retry_delay_seconds=5,
    )

    # --- Both values of a duplicated column name must be seen (try 1) ---

    # A row keyed by column name would keep only the last X and pass. Evaluated
    # positionally, the 0 is seen and the check fails.
    fail_duplicate_columns = SnowflakeCheckOperator(
        task_id="fail_duplicate_columns",
        sql="SELECT 0 AS X, 1 AS X",
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    # The reverse order, in case only the last column were being read.
    fail_duplicate_columns_reversed = SnowflakeCheckOperator(
        task_id="fail_duplicate_columns_reversed",
        sql="SELECT 1 AS X, 0 AS X",
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    # --- A NULL never satisfies a non-null pass_value (try 1) ---

    fail_null_vs_text = SnowflakeValueCheckOperator(
        task_id="fail_null_vs_text",
        sql="SELECT NULL",
        pass_value="OK",
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    # An empty string is NOT None: str('') is '', which never equals 'None'.
    fail_empty_string_vs_null = SnowflakeValueCheckOperator(
        task_id="fail_empty_string_vs_null",
        sql="SELECT ''",
        pass_value=None,
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )
