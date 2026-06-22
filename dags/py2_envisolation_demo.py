"""
Session 31 / Finding #2 live probe: environment isolation (clean-env).

Proves the task subprocess does NOT inherit orchestrator secrets, and that the
author env= knob injects the author's own vars.

  env_probe   - default PythonOperator: reports which secrets are visible in
                os.environ (expect ALL False) and that DAGS_REPO_PATH survives.
  env_override- PythonOperator(env={"PIFLOW_TEST_VAR": ...}): reports that the
                author var is present (expect True) and secrets still absent.

Return values are dicts -> json.dumps -> stored as JSON xcom (easy psql read).
"""

from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime
import os

SECRETS = ["POSTGRES_PASSWORD", "CONNECTION_PASSWORD_AES_KEY", "JWT_SECRET", "SMTP_PASSWORD"]


def env_probe(**kwargs):
    report = {s: (s in os.environ) for s in SECRETS}
    report["DAGS_REPO_PATH_present"] = "DAGS_REPO_PATH" in os.environ
    report["PATH_present"] = "PATH" in os.environ
    report["total_env_keys"] = len(os.environ)
    print(f"env_probe report: {report}")
    return report


def env_override(**kwargs):
    report = {s: (s in os.environ) for s in SECRETS}
    report["PIFLOW_TEST_VAR"] = os.environ.get("PIFLOW_TEST_VAR")
    report["DAGS_REPO_PATH_present"] = "DAGS_REPO_PATH" in os.environ
    print(f"env_override report: {report}")
    return report


with DAG(
    dag_id="py2_envisolation_demo",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="S31 #2 env isolation: secrets stripped, env= injects author vars",
) as dag:

    probe = PythonOperator(
        task_id="env_probe",
        python_callable=env_probe,
    )

    override = PythonOperator(
        task_id="env_override",
        python_callable=env_override,
        env={"PIFLOW_TEST_VAR": "present_via_env_param"},
    )

    probe >> override
