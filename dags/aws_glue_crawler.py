"""GlueCrawlerRunOperator + GlueCrawlerSensor live test.

Runs the existing crawler `maestro-test-crawler` and waits for it to finish, then
the sensor confirms it reached READY + SUCCEEDED.

Connection: `aws_test`. Needs glue:StartCrawler + glue:GetCrawler.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, GlueCrawlerRunOperator, GlueCrawlerSensor

with DAG(
    dag_id="aws_glue_crawler",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "glue", "crawler"],
) as dag:

    run_crawler = GlueCrawlerRunOperator(
        task_id="run_crawler",
        crawler_name="maestro-test-crawler",
        poll_interval=15,
        execution_timeout=900,
    )

    wait_crawler = GlueCrawlerSensor(
        task_id="wait_crawler",
        crawler_name="maestro-test-crawler",
        poke_interval=15,
        timeout=300,
        mode="poke",
    )

    run_crawler >> wait_crawler
