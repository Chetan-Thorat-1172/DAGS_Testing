"""AthenaOperator live test — query real S3 data via an external table.

Bootstraps everything through AthenaOperator: creates a database, creates an
external table over s3://maestro-pi-s3-test/EmployeeData/ (skipping the CSV
header), then runs a SELECT over it. The final query succeeding proves Athena
read the real S3 data end to end.

Connection: `aws_test`. Results: s3://maestro-pi-s3-test/athena-results/
IAM: athena:* (start/get/stop) + glue (Get/Create Database/Table, GetPartitions)
     + s3 read/write on maestro-pi-s3-test.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, AthenaOperator

DB = "maestro_test_db"
OUTPUT = "s3://maestro-pi-s3-test/athena-results/"
LOCATION = "s3://maestro-pi-s3-test/EmployeeData/"

CREATE_DB = f"CREATE DATABASE IF NOT EXISTS {DB}"

CREATE_TABLE = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DB}.employees (
  emp_id int,
  first_name string,
  last_name string,
  department string,
  designation string,
  salary int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '{LOCATION}'
TBLPROPERTIES ('skip.header.line.count'='1')
"""

COUNT_QUERY = f"SELECT count(*) AS n FROM {DB}.employees"
AGG_QUERY = f"SELECT department, count(*) AS headcount FROM {DB}.employees GROUP BY department"

with DAG(
    dag_id="aws_athena_s3_table",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "athena", "s3"],
) as dag:

    create_db = AthenaOperator(
        task_id="create_db", query=CREATE_DB, output_location=OUTPUT, sleep_time=5,
    )
    create_table = AthenaOperator(
        task_id="create_table", query=CREATE_TABLE, database=DB,
        output_location=OUTPUT, sleep_time=5,
    )
    count_rows = AthenaOperator(
        task_id="count_rows", query=COUNT_QUERY, database=DB,
        output_location=OUTPUT, sleep_time=5,
    )
    by_department = AthenaOperator(
        task_id="by_department", query=AGG_QUERY, database=DB,
        output_location=OUTPUT, sleep_time=5,
    )

    create_db >> create_table >> count_rows >> by_department
