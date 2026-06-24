"""S36 #5 b-ii live probe: data_interval_start/end populated by the scheduler and
rendered into a Python task's op_kwargs. Every-minute cron, catchup off -> one run."""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def probe(dis=None, die=None, dis_iso=None, window=None):
    return {"dis": dis, "die": die, "dis_iso": dis_iso, "window": window}


with DAG(
    dag_id="s36_dataintv_probe",
    schedule_interval="* * * * *",  # every minute
    start_date=datetime(2026, 6, 24, 0, 0),
    catchup=False,
) as dag:
    PythonOperator(
        task_id="probe",
        python_callable=probe,
        op_kwargs={
            "dis": "{{ data_interval_start }}",
            "die": "{{ data_interval_end }}",
            "dis_iso": "{{ data_interval_start.isoformat() }}",
            "window": "{{ (data_interval_end - data_interval_start).total_seconds() }}",
        },
    )
