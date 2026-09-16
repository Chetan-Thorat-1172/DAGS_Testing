"""Live end-to-end test for CopyFromExternalStageToSnowflakeOperator.

Loads files out of DEMO.PUBLIC.COPYTEST_STAGE into DEMO.PUBLIC.COPYTEST_TARGET
and COPYTEST_TARGET_COLS, and checks the result with the check operators — so
this also exercises the two operators working together, which is how they are
meant to be used.

Requires the COPYTEST_ objects from copyinto_livetest_setup.sql. The stage holds
three files:

    good/part1.csv    ID 1..5
    good/part2.csv    ID 6..10
    other/part3.csv   ID 1

The tasks run in a chain rather than in parallel, because COPY INTO records
which files it has already loaded into a table and skips them on a later run.
That is the right default for a pipeline — a retry does not duplicate data — but
it means each stage of this test has to truncate first and pass FORCE=TRUE to
re-read the same files. Which is convenient: it tests copy_options too.

A green run is the assertion. The deliberate rejections live in
snowflake_copy_into_rejects.py.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    CopyFromExternalStageToSnowflakeOperator,
    SnowflakeValueCheckOperator,
    SQLExecuteQueryOperator,
)

CONN = "snowflake_conn"
DB, SCHEMA = "DEMO", "PUBLIC"
TARGET = f"{DB}.{SCHEMA}.COPYTEST_TARGET"
TARGET_COLS = f"{DB}.{SCHEMA}.COPYTEST_TARGET_COLS"

# Every task talks to the same database and schema, so the session is pointed at
# them once instead of qualifying every name.
SESSION = {"database": DB, "schema": SCHEMA}


def truncate(task_id, table):
    return SQLExecuteQueryOperator(
        task_id=task_id,
        sql=f"TRUNCATE TABLE {table}",
        do_xcom_push=False,
    )


def row_count(task_id, table, expected):
    return SnowflakeValueCheckOperator(
        task_id=task_id,
        sql=f"SELECT COUNT(*) FROM {table}",
        pass_value=expected,
        retry_on_failure=False,
    )


with DAG(
    dag_id="snowflake_copy_into_livetest",
    description="COPY INTO from a stage, verified with value checks",
    tags=["livetest", "snowflake"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # What is actually in the stage, for the log.
    list_stage = SQLExecuteQueryOperator(
        task_id="list_stage",
        sql="LIST @DEMO.PUBLIC.COPYTEST_STAGE",
        show_return_value_in_logs=True,
    )

    # --- 1. prefix narrows the load, and file_format is given as a NAME ---

    clear_1 = truncate("clear_1", TARGET)

    load_prefix = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_prefix",
        table="COPYTEST_TARGET",
        stage="COPYTEST_STAGE",
        prefix="good/",
        file_format="COPYTEST_CSV",
        **SESSION,
    )

    # 10, not 11: other/part3.csv is outside the prefix.
    check_prefix = row_count("check_prefix", TARGET, 10)

    # --- 2. an explicit files list, and file_format given INLINE ---

    clear_2 = truncate("clear_2", TARGET)

    load_files = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_files",
        table="COPYTEST_TARGET",
        stage="COPYTEST_STAGE",
        files=["good/part1.csv"],
        file_format="(type = 'CSV', skip_header = 1, field_optionally_enclosed_by = '\"')",
        # The file was loaded a moment ago, so COPY would skip it without this.
        copy_options="FORCE=TRUE",
        **SESSION,
    )

    check_files = row_count("check_files", TARGET, 5)

    # --- 3. pattern matching ---

    clear_3 = truncate("clear_3", TARGET)

    load_pattern = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_pattern",
        table="COPYTEST_TARGET",
        stage="COPYTEST_STAGE",
        prefix="good/",
        pattern=".*part[12][.]csv",
        file_format="COPYTEST_CSV",
        copy_options="FORCE=TRUE",
        **SESSION,
    )

    check_pattern = row_count("check_pattern", TARGET, 10)

    # --- 4. columns_array maps the file's columns onto a different order ---
    #
    # COPYTEST_TARGET_COLS is (NAME, ID) while the CSV is ID,NAME. Naming the
    # columns is what makes the load land the right way round.

    clear_4 = truncate("clear_4", TARGET_COLS)

    load_columns = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_columns",
        table="COPYTEST_TARGET_COLS",
        stage="COPYTEST_STAGE",
        prefix="good/part1.csv",
        columns_array=["ID", "NAME"],
        file_format="COPYTEST_CSV",
        copy_options="FORCE=TRUE",
        **SESSION,
    )

    # If the mapping were ignored, NAME would hold the numbers.
    check_columns = SnowflakeValueCheckOperator(
        task_id="check_columns",
        sql=f"SELECT NAME FROM {TARGET_COLS} WHERE ID = 1",
        pass_value="Alice",
        retry_on_failure=False,
    )

    # --- 5. validation_mode inspects the files without loading them ---

    clear_5 = truncate("clear_5", TARGET)

    # copy_options and validation_mode are appended to the statement exactly as
    # written, so each has to be the whole clause — `VALIDATION_MODE = ...`,
    # not just the mode. Nothing is prepended for you.
    validate_only = CopyFromExternalStageToSnowflakeOperator(
        task_id="validate_only",
        table="COPYTEST_TARGET",
        stage="COPYTEST_STAGE",
        prefix="good/",
        file_format="COPYTEST_CSV",
        validation_mode="VALIDATION_MODE = RETURN_2_ROWS",
        **SESSION,
    )

    # RETURN_n_ROWS validates and returns sample rows; it loads nothing.
    check_validate_loaded_nothing = row_count("check_validate_loaded_nothing", TARGET, 0)

    # --- 6. autocommit is ON by default, so the load is durable at once ---

    clear_6 = truncate("clear_6", TARGET)

    load_default_autocommit = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_default_autocommit",
        table="COPYTEST_TARGET",
        stage="COPYTEST_STAGE",
        prefix="good/part2.csv",
        file_format="COPYTEST_CSV",
        copy_options="FORCE=TRUE",
        **SESSION,
    )

    # A different task, so a different session: the rows are only visible here
    # if the load really did commit.
    check_committed = row_count("check_committed", TARGET, 5)

    (
        list_stage
        >> clear_1 >> load_prefix >> check_prefix
        >> clear_2 >> load_files >> check_files
        >> clear_3 >> load_pattern >> check_pattern
        >> clear_4 >> load_columns >> check_columns
        >> clear_5 >> validate_only >> check_validate_loaded_nothing
        >> clear_6 >> load_default_autocommit >> check_committed
    )
