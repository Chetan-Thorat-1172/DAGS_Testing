"""Data-quality checks over DEMO.PUBLIC.SAMPLE_TABLE and DEMO.PUBLIC.EMPTY_TABLE.

    SAMPLE_TABLE  ID NUMBER, NAME VARCHAR  — 10 rows, ID 1..10, Alice..Jack
    EMPTY_TABLE   ID NUMBER, NAME VARCHAR  — 0 rows

This DAG run ends RED by design. Nine tasks are expected to pass and four are
expected to fail, because the empty table is what a check is for: catching the
load that produced nothing.

The four failures show that "empty" is not one thing:

  * an aggregate over an empty table returns a ROW CONTAINING 0, which is false;
  * a plain SELECT over an empty table returns NO ROWS AT ALL;
  * an aggregate like MAX() over an empty table returns a row containing NULL.

Each fails for its own reason and says so. Expected outcome, checked against
metadata.task_instance afterwards — every failure terminal at try_number 1,
despite retries=2, because a re-read cannot change an empty table:

    SELECT task_id, state, try_number
    FROM metadata.task_instance
    WHERE dag_id = 'snowflake_check_demo_tables'
    ORDER BY state, task_id;
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    SnowflakeCheckOperator,
    SnowflakeValueCheckOperator,
)

CONN = "snowflake_conn"
SAMPLE = "DEMO.PUBLIC.SAMPLE_TABLE"
EMPTY = "DEMO.PUBLIC.EMPTY_TABLE"

with DAG(
    dag_id="snowflake_check_demo_tables",
    description="Checks over a populated and an empty table — four tasks fail by design",
    tags=["livetest", "snowflake"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # ------------------------------------------------------------------
    # SAMPLE_TABLE — 10 good rows. All of these pass.
    # ------------------------------------------------------------------

    # The everyday check: did anything land?
    sample_has_rows = SnowflakeCheckOperator(
        task_id="sample_has_rows",
        sql=f"SELECT COUNT(*) FROM {SAMPLE}",
    )

    # The exact count is known, so assert it.
    sample_row_count = SnowflakeValueCheckOperator(
        task_id="sample_row_count",
        sql=f"SELECT COUNT(*) FROM {SAMPLE}",
        pass_value=10,
    )

    # A daily load would not land on 10 exactly; tolerance is the usual shape.
    sample_row_count_tolerance = SnowflakeValueCheckOperator(
        task_id="sample_row_count_tolerance",
        sql=f"SELECT COUNT(*) FROM {SAMPLE}",
        pass_value=10,
        tolerance=0.2,  # 8..12
    )

    # Completeness and uniqueness in one task: every value in the row must be
    # true, so one check can carry a whole set of invariants and name the one
    # that broke.
    sample_invariants = SnowflakeCheckOperator(
        task_id="sample_invariants",
        sql=f"""
            SELECT
                COUNT(*) > 0                     AS HAS_ROWS,
                COUNT(*) = COUNT(DISTINCT ID)    AS IDS_UNIQUE,
                COUNT_IF(NAME IS NULL) = 0       AS NAMES_COMPLETE,
                MIN(ID) = 1                      AS STARTS_AT_ONE,
                MAX(ID) = 10                     AS ENDS_AT_TEN
            FROM {SAMPLE}
        """,
    )

    # `= 0` is the idiom for "nothing is broken": a bare COUNT of the bad rows
    # returns 0 when the data is CLEAN, and 0 is false — which would fail the
    # check exactly when it should pass.
    sample_no_null_names = SnowflakeCheckOperator(
        task_id="sample_no_null_names",
        sql=f"SELECT COUNT(*) = 0 FROM {SAMPLE} WHERE NAME IS NULL",
    )

    # A bind parameter — the name is passed as a value, never spliced into SQL.
    sample_contains_alice = SnowflakeCheckOperator(
        task_id="sample_contains_alice",
        sql=f"SELECT COUNT(*) FROM {SAMPLE} WHERE NAME = :name",
        parameters={"name": "Alice"},
    )

    sample_first_name = SnowflakeValueCheckOperator(
        task_id="sample_first_name",
        sql=f"SELECT NAME FROM {SAMPLE} WHERE ID = 1",
        pass_value="Alice",
    )

    # Session overrides let the SQL go unqualified — useful when the same check
    # runs against dev, staging and prod.
    sample_via_session_override = SnowflakeCheckOperator(
        task_id="sample_via_session_override",
        sql="SELECT COUNT(*) FROM SAMPLE_TABLE",
        database="DEMO",
        schema="PUBLIC",
    )

    # ------------------------------------------------------------------
    # EMPTY_TABLE — 0 rows. This is what checks exist to catch.
    # ------------------------------------------------------------------

    # EXPECTED TO FAIL. The headline case: COUNT(*) over an empty table returns
    # a row containing 0, and 0 is false. This is the check you put after a load
    # to stop an empty result reaching anything downstream.
    empty_table_caught = SnowflakeCheckOperator(
        task_id="empty_table_caught",
        sql=f"SELECT COUNT(*) FROM {EMPTY}",
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    # EXPECTED TO FAIL, for a different reason. A plain SELECT over an empty
    # table returns no rows at all — there is nothing to evaluate, which is its
    # own failure and reported as such.
    empty_table_no_rows_at_all = SnowflakeCheckOperator(
        task_id="empty_table_no_rows_at_all",
        sql=f"SELECT ID, NAME FROM {EMPTY}",
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    # EXPECTED TO FAIL, for a third reason. MAX() over an empty table returns a
    # row containing NULL, and NULL is false.
    empty_table_null_aggregate = SnowflakeCheckOperator(
        task_id="empty_table_null_aggregate",
        sql=f"SELECT MAX(ID) FROM {EMPTY}",
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    # EXPECTED TO FAIL. The value check's version: 0 rows where 10 were wanted.
    empty_table_wrong_count = SnowflakeValueCheckOperator(
        task_id="empty_table_wrong_count",
        sql=f"SELECT COUNT(*) FROM {EMPTY}",
        pass_value=10,
        retry_on_failure=False,
        retries=2,
        retry_delay_seconds=5,
    )

    # PASSES. When a table is MEANT to be empty — a staging table drained after
    # a load, a rejects table with nothing in it — assert that explicitly.
    empty_table_is_empty_as_expected = SnowflakeCheckOperator(
        task_id="empty_table_is_empty_as_expected",
        sql=f"SELECT COUNT(*) = 0 FROM {EMPTY}",
    )

    # PASSES. The same assertion as a value check.
    empty_table_count_is_zero = SnowflakeValueCheckOperator(
        task_id="empty_table_count_is_zero",
        sql=f"SELECT COUNT(*) FROM {EMPTY}",
        pass_value=0,
    )

    # ------------------------------------------------------------------
    # Both tables in one check.
    # ------------------------------------------------------------------

    # PASSES. The shape a real pipeline uses: the source has rows, the staging
    # table has been drained, and the two are consistent.
    cross_table_state = SnowflakeCheckOperator(
        task_id="cross_table_state",
        sql=f"""
            SELECT
                (SELECT COUNT(*) FROM {SAMPLE}) > 0  AS SOURCE_POPULATED,
                (SELECT COUNT(*) FROM {EMPTY}) = 0   AS STAGING_DRAINED,
                (SELECT COUNT(*) FROM {SAMPLE}) >
                    (SELECT COUNT(*) FROM {EMPTY})   AS SOURCE_BIGGER
        """,
    )
