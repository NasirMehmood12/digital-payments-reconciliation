"""Deterministic synthetic inputs containing deliberate reconciliation breaks."""
import csv
import json
import random
from pathlib import Path

SETTLEMENT_FIELDS = ['settlement_id','payment_id','gross_minor','fee_minor','net_minor','currency','settled_at']


def generate(root: Path, seed=42):
    rng = random.Random(seed)
    (root/'events').mkdir(parents=True, exist_ok=True)
    (root/'settlements').mkdir(parents=True, exist_ok=True)
    events, settlements = [], []
    for i in range(1, 101):
        transaction = f'PAY{i:04d}'
        amount = rng.randint(500, 20000) * 100
        status = 'FAILED' if i % 20 == 0 else 'SUCCESS'
        events.append(dict(event_id=f'EVT{i:04d}',transaction_id=transaction,event_type='PAYMENT',
            original_payment_id=None,status=status,amount_minor=amount,currency='PKR',occurred_at='2026-01-05T10:00:00+00:00'))
        if status == 'SUCCESS' and i not in (1,3):
            fee = amount // 100
            gross = amount - 10000 if i == 2 else amount
            settlements.append(dict(settlement_id=f'SET{i:04d}',payment_id=transaction,gross_minor=gross,
                fee_minor=fee,net_minor=gross-fee,currency='USD' if i == 5 else 'PKR',settled_at='2026-01-06T12:00:00+00:00'))
    # Exact replay of one source event; must not create a second transaction.
    events.append(dict(events[9]))
    events.append(dict(event_id='REFEVT1',transaction_id='REF0001',event_type='REFUND',original_payment_id='UNKNOWN',
        status='SUCCESS',amount_minor=25000,currency='PKR',occurred_at='2026-01-06T10:00:00+00:00'))
    events.append(dict(event_id='REFEVT2',transaction_id='REF0002',event_type='REFUND',original_payment_id='PAY0008',
        status='SUCCESS',amount_minor=events[7]['amount_minor']+10000,currency='PKR',occurred_at='2026-01-06T10:00:00+00:00'))
    duplicate = dict(next(s for s in settlements if s['payment_id']=='PAY0007'))
    duplicate['settlement_id']='SET_DUPLICATE_7'
    settlements.append(duplicate)
    settlements.append(dict(settlement_id='SET_ORPHAN',payment_id='UNKNOWN',gross_minor=100000,
        fee_minor=1000,net_minor=99000,currency='PKR',settled_at='2026-01-06T12:00:00+00:00'))
    late_amount=events[2]['amount_minor']
    late=[dict(settlement_id='SET0003',payment_id='PAY0003',gross_minor=late_amount,
        fee_minor=late_amount//100,net_minor=late_amount-late_amount//100,currency='PKR',settled_at='2026-01-08T12:00:00+00:00')]
    (root/'events'/'payments.jsonl').write_text(''.join(json.dumps(e,sort_keys=True)+'\n' for e in events),encoding='utf-8')
    for filename, records in [('settlements_initial.csv',settlements),('settlements_late.csv',late)]:
        with (root/'settlements'/filename).open('w',newline='',encoding='utf-8') as f:
            writer=csv.DictWriter(f,fieldnames=SETTLEMENT_FIELDS);writer.writeheader();writer.writerows(records)
    return events,settlements,late
