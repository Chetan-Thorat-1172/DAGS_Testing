"""
Test DAG: Multiple Outputs (@task with multiple_outputs=True)

Tests that @task(multiple_outputs=True) pushes each dict key as a separate
XCom entry, and downstream tasks can pull individual keys via result["key"].

Flow:
  get_config() -> use_host(host) -> use_port(port)

Expected:
  1. get_config returns {"host": "snowflake.com", "port": 443, "protocol": "https"}
  2. Each key is pushed as a separate XCom entry
  3. use_host receives only "snowflake.com" via XCom key="host"
  4. use_port receives only 443 via XCom key="port"
"""

from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, task, dag


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False,
     description="Tests @task(multiple_outputs=True)", tags=["test", "taskflow"])
def test_multiple_outputs():

    @task(multiple_outputs=True)
    def get_config():
        """Returns a dict - each key becomes a separate XCom entry."""
        config = {"host": "snowflake.com", "port": 443, "protocol": "https"}
        print(f"Returning config: {config}")
        return config

    @task
    def use_host(host):
        """Receives only the 'host' key from get_config output."""
        print(f"Host received: {host}")
        assert host == "snowflake.com", f"Expected 'snowflake.com', got '{host}'"
        return f"connected to {host}"

    @task
    def use_port(port):
        """Receives only the 'port' key from get_config output."""
        print(f"Port received: {port}")
        assert port == 443, f"Expected 443, got {port}"
        return f"using port {port}"

    config = get_config()
    use_host(config["host"])
    use_port(config["port"])


test_multiple_outputs()
