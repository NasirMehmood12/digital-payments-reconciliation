"""Contract tests: acknowledgement must follow durable persistence, including crash/replay."""
import json
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from payments.streaming import process_message


class StreamingTests(unittest.TestCase):
    def setUp(self):
        self.event=dict(event_id='E1',transaction_id='P1',event_type='PAYMENT',original_payment_id=None,
            status='SUCCESS',amount_minor=500000,currency='PKR',occurred_at='2026-01-05T10:00:00Z')
        self.raw=json.dumps(self.event).encode()
        self.coordinates=dict(topic='payments.events.v1',partition=0,offset=41,group='test')

    def test_database_before_offset_and_exact_next_offset(self):
        calls=[]
        def persist(event,coordinates):calls.append('db_commit');return 'INSERTED'
        result=process_message(self.raw,self.coordinates,persist,lambda n:calls.append(('ack',n)))
        self.assertEqual(result,'INSERTED')
        self.assertEqual(calls,['db_commit',('ack',42)])

    def test_db_failure_does_not_acknowledge(self):
        acknowledgements=[]
        def fail(*args):raise RuntimeError('database failed')
        with self.assertRaises(RuntimeError):
            process_message(self.raw,self.coordinates,fail,acknowledgements.append)
        self.assertEqual(acknowledgements,[])

    def test_crash_commits_db_but_not_kafka(self):
        calls=[]
        def persist(*args):calls.append('db_commit');return 'INSERTED'
        with self.assertRaises(SystemExit) as error:
            process_message(self.raw,self.coordinates,persist,lambda n:calls.append('ack'),crash_after_db=True)
        self.assertEqual(error.exception.code,9)
        self.assertEqual(calls,['db_commit'])

    def test_replay_after_crash_acknowledges_without_second_insert(self):
        saved=set();acks=[];inserts=[]
        def persist(event,coordinates):
            if event['event_id'] in saved:return 'DB_REPLAY'
            saved.add(event['event_id']);inserts.append(event['event_id']);return 'INSERTED'
        with self.assertRaises(SystemExit):
            process_message(self.raw,self.coordinates,persist,acks.append,crash_after_db=True)
        result=process_message(self.raw,self.coordinates,persist,acks.append)
        self.assertEqual(result,'DB_REPLAY');self.assertEqual(inserts,['E1']);self.assertEqual(acks,[42])

    def test_invalid_json_never_persists_or_acknowledges(self):
        calls=[]
        with self.assertRaises(ValueError):
            process_message(b'invalid',self.coordinates,lambda *a:calls.append('db'),calls.append)
        self.assertEqual(calls,[])

    def test_invalid_event_never_persists_or_acknowledges(self):
        calls=[];self.event['amount_minor']=-1
        with self.assertRaises(ValueError):
            process_message(json.dumps(self.event).encode(),self.coordinates,lambda *a:calls.append('db'),calls.append)
        self.assertEqual(calls,[])


if __name__=='__main__':unittest.main()
