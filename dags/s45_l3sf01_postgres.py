from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PostgresOperator

# S45 / L3-SF-01 live proof (Postgres path).
# Before the fix, PostgresOperator (a SQLExecuteQueryOperator subclass) raised
# at DAG parse, so this DAG could not be ingested at all. Now it parses, runs
# via SQLExecutor -> lib/pq against the in-cluster Postgres sidecar, and pushes
# the first result row to XCOM.
with DAG(
    dag_id="s45_l3sf01_postgres",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 29),
    catchup=False,
    description="L3-SF-01 proof: generic SQL family (Postgres) is now authorable + runnable",
) as dag:
    PostgresOperator(
        task_id="pg_select",
        sql="SELECT 42 AS answer",
        postgres_conn_id="pg_sidecar",
    )
