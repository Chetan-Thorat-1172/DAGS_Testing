"""Live test: how SnowflakeSqlApiOperator reports a failure.

Every task fails by design — a red run is correct. The SQL REST API reports a
failed statement through its status rather than as an HTTP error, so what is
being verified is that the detail survives into the task error: Snowflake's
message, its error code and its SQLSTATE.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, SnowflakeSqlApiOperator

CONN = "snowflake_conn"
SESSION = {"database": "DEMO", "schema": "PUBLIC"}

with DAG(
    dag_id="snowflake_sql_api_rejects",
    description="SQL REST API failures — every task fails by design",
    tags=["livetest", "snowflake", "sqlapi"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # A table that does not exist: a compilation error, reported through the
    # statement's status rather than as an HTTP failure.
    missing_table = SnowflakeSqlApiOperator(
        task_id="missing_table",
        sql="SELECT * FROM DEMO.PUBLIC.NO_SUCH_TABLE_XYZ",
        statement_count=1,
        poll_interval=2,
        **SESSION,
    )

    # Syntax Snowflake cannot parse.
    bad_syntax = SnowflakeSqlApiOperator(
        task_id="bad_syntax",
        sql="SELCT 1",
        statement_count=1,
        poll_interval=2,
        **SESSION,
    )

    # statement_count must match the number of statements sent. Saying 2 when
    # three are supplied is rejected at submission.
    count_mismatch = SnowflakeSqlApiOperator(
        task_id="count_mismatch",
        sql="SELECT 1; SELECT 2; SELECT 3;",
        statement_count=2,
        poll_interval=2,
        **SESSION,
    )

    # A statement that fails partway: the first insert is fine, the second
    # divides by zero. The task must fail rather than report success because
    # the request as a whole was accepted.
    fails_midway = SnowflakeSqlApiOperator(
        task_id="fails_midway",
        sql=(
            "SELECT 1; "
            "SELECT 1/0; "
            "SELECT 3;"
        ),
        statement_count=3,
        poll_interval=2,
        **SESSION,
    )
