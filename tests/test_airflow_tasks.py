import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from payments.airflow_tasks import make_task_env


class AirflowEnvironmentTests(unittest.TestCase):
    def config(self):
        return dict(DB_HOST='127.0.0.1',DB_PORT='5432',DB_NAME='payments_recon_db',DB_USER='example_user',DB_PASSWORD='example-not-real')

    def test_loan_settings_do_not_leak_to_payments(self):
        parent=dict(DB_NAME='loan_risk_db',DB_PASSWORD='other-example',DB_SCHEMA='risk',PATH='/bin')
        env=make_task_env(self.config(),parent)
        self.assertEqual(env['DB_NAME'],'payments_recon_db')
        self.assertEqual(env['DB_PASSWORD'],'example-not-real')
        self.assertNotIn('DB_SCHEMA',env)
        self.assertEqual(env['DB_HOST'],'host.docker.internal')
        self.assertEqual(parent['DB_NAME'],'loan_risk_db')
        self.assertEqual(env['PATH'],'/bin')

    def test_missing_password_fails_before_execution(self):
        config=self.config();config['DB_PASSWORD']=''
        with self.assertRaises(ValueError):make_task_env(config,{})

    def test_wrong_database_is_rejected(self):
        config=self.config();config['DB_NAME']='loan_risk_db'
        with self.assertRaises(ValueError):make_task_env(config,{})

    def test_invalid_port_is_rejected(self):
        config=self.config();config['DB_PORT']='99999'
        with self.assertRaises(ValueError):make_task_env(config,{})


if __name__=='__main__':unittest.main()
