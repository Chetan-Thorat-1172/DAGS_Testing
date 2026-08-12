from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksRunNowOperator

# T3: job_parameters with a templated value, plus the trigger-conf fallback.
#
# Part A: explicit job_parameters with a template — run_label gets the DAG's
#         logical date string. Proves parameter merge and Go template rendering
#         inside a nested map.
#
# Part B: no job_parameters set — the task's conf forwarding kicks in.
#         Trigger this DAG run with conf {"run_label": "from_conf"} and the
#         job will receive that as job_parameters. Trigger without conf and
#         the job runs with its own Databricks default ("maestro_default").
with DAG(
    dag_id="databricks_rn_t3_params",
    schedule=None,
    start_date=datetime(2026, 8, 12),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 1,
    },
    tags=["databricks", "run-now"],
) as dag:
    explicit_params = DatabricksRunNowOperator(
        task_id="explicit_job_parameters",
        job_id=632128058992973,
        job_parameters={"run_label": "maestro_{{ .DS }}"},
        polling_period_seconds=10,
    )

    conf_fallback = DatabricksRunNowOperator(
        task_id="conf_fallback_to_job_parameters",
        job_id=632128058992973,
        # job_parameters deliberately omitted — will forward the trigger conf
        polling_period_seconds=10,
    )

    explicit_params >> conf_fallback
