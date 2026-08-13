from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSQLStatementsOperator

# T2: DML. INSERT then MERGE.
#
# MERGE is the statement the durable-reattach feature exists for: running it twice
# against the same table is not idempotent in general, so this is the realistic
# shape of what consumers will run.
with DAG(
    dag_id="databricks_sql_t2_dml",
    schedule=None,
    start_date=datetime(2026, 8, 13),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 0,
    },
    tags=["databricks", "sql"],
) as dag:
    insert_rows = DatabricksSQLStatementsOperator(
        task_id="insert_rows",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        statement="""
            INSERT INTO events VALUES
                (1, 'alpha', DATE'2026-08-13'),
                (2, 'beta',  DATE'2026-08-13')
        """,
        poll_interval=5,
    )

    merge_rows = DatabricksSQLStatementsOperator(
        task_id="merge_rows",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        statement="""
            MERGE INTO events AS t
            USING (
                SELECT 2 AS id, 'beta_updated' AS label, DATE'2026-08-13' AS ds
                UNION ALL
                SELECT 3 AS id, 'gamma'        AS label, DATE'2026-08-13' AS ds
            ) AS s
            ON t.id = s.id
            WHEN MATCHED     THEN UPDATE SET t.label = s.label
            WHEN NOT MATCHED THEN INSERT (id, label, ds) VALUES (s.id, s.label, s.ds)
        """,
        poll_interval=5,
    )

    insert_rows >> merge_rows
