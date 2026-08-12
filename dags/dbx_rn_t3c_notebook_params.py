from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksRunNowOperator

# T3c: notebook_params fills notebook widgets directly.
# The maestro_smoke notebook reads the "ds" widget with dbutils.widgets.get("ds").
# Passing notebook_params={"ds": "{{ .DS }}"} should fill that widget and make
# the notebook exit with "ok:2026-08-12", proving the difference from job_parameters.
with DAG(
    dag_id="databricks_rn_t3c_notebook_params",
    schedule=None,
    start_date=datetime(2026, 8, 12),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 1,
    },
    tags=["databricks", "run-now"],
) as dag:
    DatabricksRunNowOperator(
        task_id="trigger_with_notebook_params",
        job_id=632128058992973,
        notebook_params={"ds": "{{ .DS }}"},
        polling_period_seconds=10,
    )
