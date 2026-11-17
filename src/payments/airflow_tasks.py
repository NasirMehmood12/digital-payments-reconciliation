"""Task helpers for the shared loan-risk Airflow stack. Credentials stay task-local."""
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DB_KEYS = ('DB_HOST','DB_PORT','DB_NAME','DB_USER','DB_PASSWORD')


def make_task_env(config, parent=None):
    missing=[key for key in DB_KEYS if not config.get(key)]
    if missing:raise ValueError('Payments .env missing: '+', '.join(missing))
    if config['DB_NAME']!='payments_recon_db':
        raise ValueError('Expected DB_NAME=payments_recon_db in the payments .env')
    port=int(config['DB_PORT'])
    if not 1<=port<=65535:raise ValueError('DB_PORT is out of range')
    # Do not inherit the loan project's database settings in the child process.
    env={key:value for key,value in (os.environ if parent is None else parent).items() if not key.startswith('DB_')}
    env.update({key:str(config[key]) for key in DB_KEYS})
    env['DB_HOST']='host.docker.internal'
    env['PYTHONUNBUFFERED']='1'
    return env


def task_env():
    from dotenv import dotenv_values
    path=ROOT/'.env'
    if not path.is_file():raise FileNotFoundError('Payments project .env was not mounted')
    # Disable interpolation so inherited loan variables cannot fill credential values.
    return make_task_env(dotenv_values(path,interpolate=False))


def run_cli(*arguments):
    subprocess.run([sys.executable,str(ROOT/'scripts/run.py'),*arguments],
                   cwd=ROOT,env=task_env(),check=True,timeout=1200)


def check_inputs():
    for folder in ('src','scripts','data/settlements'):
        if not (ROOT/folder).is_dir():raise FileNotFoundError('Missing payments path: '+folder)
    if not list((ROOT/'data/settlements').glob('*.csv')):
        raise ValueError('No settlement CSV files found in the payments data/settlements folder')
    # This check runs against the payments DB in a separate process, with its own env.
    code="""
from payments.storage import connect
connection=connect()
try:
    with connection.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM recon.payment_event')
        count=cursor.fetchone()[0]
        if count == 0: raise ValueError('No payment events; ingest them before reconciliation')
        cursor.execute('SELECT 1 FROM recon.settlement LIMIT 0')
    print('Payments database connection OK; unique events:', count)
finally:
    connection.close()
"""
    env=task_env()
    env['PYTHONPATH']=str(ROOT/'src')+os.pathsep+env.get('PYTHONPATH','')
    subprocess.run([sys.executable,'-c',code],cwd=ROOT,env=env,check=True,timeout=60)


def load_settlements():
    files=sorted((ROOT/'data/settlements').glob('*.csv'))
    if not files:raise ValueError('No settlement CSVs available')
    for path in files:
        run_cli('ingest-settlements',str(path))


def reconcile_now():
    # Operational processing time: existing January demo timestamps move forward to now.
    run_cli('reconcile','--as-of',datetime.now(timezone.utc).isoformat())
