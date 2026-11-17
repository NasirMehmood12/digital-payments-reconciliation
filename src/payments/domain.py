"""Pure business rules. Money uses integer minor units; no floating point math."""
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from collections import defaultdict

RULE_VERSION = 'DEMO_V1'


def timestamp(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Timestamps must include a timezone')
    return result.astimezone(timezone.utc)


def deadline(event):
    # Illustrative contract: next calendar day at 17:00 UTC, including weekends.
    day = timestamp(event['occurred_at']).date() + timedelta(days=1)
    return datetime.combine(day, time(17), tzinfo=timezone.utc)


@dataclass(frozen=True)
class Finding:
    entity_id: str
    rule_code: str
    status: str
    explanation: str
    amount_minor: int
    currency: str

    @property
    def key(self):
        return f'{RULE_VERSION}:{self.entity_id}:{self.rule_code}'


def reconcile(events, settlements, as_of):
    """An as-of business-event-time evaluation over currently ingested data."""
    as_of = timestamp(as_of)
    events = [e for e in events if timestamp(e['occurred_at']) <= as_of]
    settlements = [s for s in settlements if timestamp(s['settled_at']) <= as_of]
    payments = {e['transaction_id']: e for e in events if e['event_type'] == 'PAYMENT'}
    by_payment = defaultdict(list)
    for row in settlements:
        by_payment[row['payment_id']].append(row)
    findings = []

    def add(entity, code, status, reason, amount, currency):
        findings.append(Finding(entity, code, status, reason, amount, currency))

    for payment_id, event in sorted(payments.items()):
        matches = by_payment.get(payment_id, [])
        amount, currency = event['amount_minor'], event['currency']
        if event['status'] != 'SUCCESS':
            if matches:
                add(payment_id, 'UNEXPECTED_SETTLEMENT', 'EXCEPTION', 'A failed payment has settlement records.', amount, currency)
            continue
        if not matches:
            status = 'EXCEPTION' if as_of > deadline(event) else 'PENDING'
            add(payment_id, 'MISSING_SETTLEMENT', status, f'No settlement received. Due by {deadline(event).isoformat()}.', amount, currency)
        elif len(matches) > 1:
            add(payment_id, 'DUPLICATE_SETTLEMENT', 'EXCEPTION', f'{len(matches)} distinct settlement IDs refer to this payment.', amount, currency)
        else:
            settled = matches[0]
            if settled['currency'] != currency:
                add(payment_id, 'CURRENCY_MISMATCH', 'EXCEPTION', 'Settlement currency differs from payment currency; amounts were not compared across currencies.', amount, currency)
            else:
                expected_fee = amount // 100  # Illustrative 1% fee, rounded down to minor units.
                if (settled['gross_minor'] != amount or settled['fee_minor'] != expected_fee
                        or settled['net_minor'] != amount - expected_fee):
                    add(payment_id, 'AMOUNT_MISMATCH', 'EXCEPTION',
                        f"Expected gross/fee/net {amount}/{expected_fee}/{amount-expected_fee}; received {settled['gross_minor']}/{settled['fee_minor']}/{settled['net_minor']} minor units.", amount, currency)
                else:
                    late = timestamp(settled['settled_at']) > deadline(event)
                    add(payment_id, 'SETTLEMENT_MATCH', 'MATCHED_LATE' if late else 'MATCHED',
                        'Amounts and currency match; settlement arrived after the deadline.' if late else 'Amounts, currency, and settlement deadline match.', amount, currency)

    for payment_id, rows in sorted(by_payment.items()):
        if payment_id not in payments:
            for row in rows:
                add(f"settlement/{row['settlement_id']}", 'UNMATCHED_SETTLEMENT', 'EXCEPTION',
                    f'No payment event found for {payment_id}.', row['gross_minor'], row['currency'])

    refunds = defaultdict(list)
    for event in events:
        if event['event_type'] == 'REFUND' and event['status'] == 'SUCCESS':
            refunds[event['original_payment_id']].append(event)
    for payment_id, rows in sorted(refunds.items()):
        parent = payments.get(payment_id)
        valid = []
        for refund in rows:
            entity = f"refund/{refund['transaction_id']}"
            if parent is None or parent['status'] != 'SUCCESS':
                add(entity, 'UNMATCHED_REFUND', 'EXCEPTION', 'Original successful payment is missing.', refund['amount_minor'], refund['currency'])
            elif parent['currency'] != refund['currency']:
                add(entity, 'REFUND_CURRENCY_MISMATCH', 'EXCEPTION', 'Refund and original payment currencies differ.', refund['amount_minor'], refund['currency'])
            elif timestamp(refund['occurred_at']) < timestamp(parent['occurred_at']):
                add(entity, 'REFUND_BEFORE_PAYMENT', 'EXCEPTION', 'Refund event predates its original payment.', refund['amount_minor'], refund['currency'])
            else:
                valid.append(refund)
        total = sum(r['amount_minor'] for r in valid)
        if parent and total > parent['amount_minor']:
            add(f'refunds/{payment_id}', 'EXCESS_REFUND', 'EXCEPTION',
                f"Total valid-currency refunds {total} exceed original payment {parent['amount_minor']} minor units.", total, parent['currency'])
    return findings
