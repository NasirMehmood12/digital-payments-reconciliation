import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from payments.demo import generate
from payments.domain import reconcile,timestamp
from payments.storage import fingerprint,read_source,validate


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.events,self.settlements,self.late=generate(self.root)
        self.addCleanup(self.temp.cleanup)

    def findings(self,as_of='2026-01-07T18:00:00Z',late=False):
        return reconcile(self.events,self.settlements+(self.late if late else []),as_of)

    def test_deliberate_breaks_and_counts(self):
        findings=self.findings()
        breaks=[f for f in findings if f.status=='EXCEPTION']
        self.assertEqual(len(breaks),8)
        self.assertEqual(sum(f.status=='MATCHED' for f in findings),90)
        self.assertEqual({f.rule_code for f in breaks},{'MISSING_SETTLEMENT','AMOUNT_MISMATCH','CURRENCY_MISMATCH',
            'DUPLICATE_SETTLEMENT','UNMATCHED_SETTLEMENT','UNMATCHED_REFUND','EXCESS_REFUND'})
        self.assertEqual(len({f.key for f in findings}),len(findings))

    def test_late_settlement_resolves_missing_key(self):
        before={f.key for f in self.findings() if f.status=='EXCEPTION'}
        after=self.findings(as_of='2026-01-09T18:00:00Z',late=True)
        after_keys={f.key for f in after if f.status=='EXCEPTION'}
        self.assertEqual(before-after_keys,{'DEMO_V1:PAY0003:MISSING_SETTLEMENT'})
        self.assertEqual(len(after_keys),7)
        self.assertEqual(sum(f.status=='MATCHED_LATE' for f in after),1)

    def test_future_settlement_is_not_used_early(self):
        finding=next(f for f in self.findings(late=True) if f.entity_id=='PAY0003')
        self.assertEqual(finding.rule_code,'MISSING_SETTLEMENT')

    def test_missing_not_exception_until_deadline(self):
        findings=self.findings(as_of='2026-01-06T17:00:00Z')
        self.assertEqual(next(f for f in findings if f.entity_id=='PAY0001').status,'PENDING')
        self.assertEqual(next(f for f in self.findings(as_of='2026-01-06T17:00:01Z') if f.entity_id=='PAY0001').status,'EXCEPTION')

    def test_no_double_count_of_duplicate_payment_event(self):
        one=[e for i,e in enumerate(self.events) if i!=100]
        self.assertEqual(reconcile(one,self.settlements,'2026-01-07T18:00:00Z'),self.findings())

    def test_integer_money_and_float_rejection(self):
        row=dict(self.events[0]);row['amount_minor']=1.5
        with self.assertRaises(ValueError):validate(row,'events')
        row['amount_minor']=True
        with self.assertRaises(ValueError):validate(row,'events')

    def test_read_inputs_and_replay_fingerprint(self):
        _,rows=read_source(self.root/'events'/'payments.jsonl','events')
        self.assertEqual(len(rows),103)
        self.assertEqual(fingerprint(rows[9]),fingerprint(rows[100]))
        _,rows=read_source(self.root/'settlements'/'settlements_initial.csv','settlements')
        self.assertEqual(len(rows),95)

    def test_deterministic_generation(self):
        before=(self.root/'events'/'payments.jsonl').read_bytes()
        generate(self.root)
        self.assertEqual(before,(self.root/'events'/'payments.jsonl').read_bytes())

    def test_naive_time_rejected(self):
        with self.assertRaises(ValueError):timestamp('2026-01-06T12:00:00')

    def test_failed_payment_settlement_is_unexpected(self):
        event=next(e for e in self.events if e['status']=='FAILED')
        row=dict(self.settlements[0]);row['payment_id']=event['transaction_id']
        findings=reconcile([event],[row],'2026-01-07T18:00:00Z')
        self.assertEqual(findings[0].rule_code,'UNEXPECTED_SETTLEMENT')


if __name__=='__main__':unittest.main()
