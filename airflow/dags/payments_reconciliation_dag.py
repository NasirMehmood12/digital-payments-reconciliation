"""Payments settlement orchestration inside the existing loan-risk Airflow stack."""
from datetime import timedelta
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def run_payments_step(step):
    import sys
    sys.path.insert(0,'/opt/payments/src')
    from payments import airflow_tasks
    steps={
        'check_inputs':airflow_tasks.check_inputs,
        'load_settlements':airflow_tasks.load_settlements,
        'reconcile_now':airflow_tasks.reconcile_now,
    }
    steps[step]()


with DAG(
    dag_id='payments_settlement_reconciliation',
    description='Load settlement files and reconcile payments in payments_recon_db.',
    start_date=pendulum.datetime(2026,1,1,tz='Asia/Karachi'),
    schedule='*/30 * * * *',
    catchup=False,
    max_active_runs=1,
    default_args={'retries':2,'retry_delay':timedelta(minutes=5)},
    dagrun_timeout=timedelta(hours=2),
    tags=['payments','reconciliation','banking'],
) as dag:
    tasks=[]
    for step in ('check_inputs','load_settlements','reconcile_now'):
        tasks.append(PythonOperator(
            task_id=step,
            python_callable=run_payments_step,
            op_kwargs={'step':step},
            execution_timeout=timedelta(minutes=30),
            do_xcom_push=False,
        ))
    tasks[0] >> tasks[1] >> tasks[2]
