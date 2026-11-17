CREATE SCHEMA IF NOT EXISTS recon;

-- One immutable final event per transaction; identical event replays are deduplicated.
CREATE TABLE IF NOT EXISTS recon.payment_event (
    event_id TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL CHECK (event_type IN ('PAYMENT', 'REFUND')),
    original_payment_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency TEXT NOT NULL CHECK (currency IN ('PKR', 'USD')),
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    source_batch_id TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((event_type = 'REFUND' AND original_payment_id IS NOT NULL)
        OR (event_type = 'PAYMENT' AND original_payment_id IS NULL))
);
CREATE INDEX IF NOT EXISTS ix_payment_parent ON recon.payment_event(original_payment_id);

CREATE TABLE IF NOT EXISTS recon.settlement (
    settlement_id TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL,
    gross_minor BIGINT NOT NULL CHECK (gross_minor >= 0),
    fee_minor BIGINT NOT NULL CHECK (fee_minor >= 0),
    net_minor BIGINT NOT NULL CHECK (net_minor >= 0),
    currency TEXT NOT NULL CHECK (currency IN ('PKR', 'USD')),
    settled_at TIMESTAMPTZ NOT NULL,
    payload_hash TEXT NOT NULL,
    source_batch_id TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_settlement_payment ON recon.settlement(payment_id);

CREATE TABLE IF NOT EXISTS recon.ingestion_batch (
    batch_id TEXT PRIMARY KEY,
    source_kind TEXT NOT NULL,
    source_file TEXT NOT NULL,
    rows_read INTEGER NOT NULL,
    rows_inserted INTEGER NOT NULL,
    duplicate_rows INTEGER NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (rows_read = rows_inserted + duplicate_rows)
);

CREATE TABLE IF NOT EXISTS recon.reconciliation_run (
    run_id UUID PRIMARY KEY,
    as_of TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    findings_count INTEGER NOT NULL,
    exception_count INTEGER NOT NULL,
    matched_payment_count INTEGER NOT NULL,
    matched_late_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS recon.reconciliation_result (
    run_id UUID NOT NULL REFERENCES recon.reconciliation_run(run_id),
    finding_key TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    rule_code TEXT NOT NULL,
    status TEXT NOT NULL,
    explanation TEXT NOT NULL,
    amount_minor BIGINT NOT NULL,
    currency TEXT NOT NULL,
    PRIMARY KEY(run_id, finding_key)
);

CREATE TABLE IF NOT EXISTS recon.exception (
    finding_key TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL,
    rule_code TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('OPEN', 'RESOLVED')),
    explanation TEXT NOT NULL,
    amount_minor BIGINT NOT NULL,
    currency TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    last_run_id UUID NOT NULL REFERENCES recon.reconciliation_run(run_id)
);

CREATE OR REPLACE VIEW recon.vw_open_exceptions AS
SELECT finding_key, entity_id, rule_code, explanation,
       amount_minor / 100.0 AS amount_major, currency, opened_at, updated_at
FROM recon.exception WHERE status = 'OPEN';

CREATE OR REPLACE VIEW recon.vw_run_summary AS
SELECT run_id, as_of, completed_at, findings_count, exception_count,
       matched_payment_count, matched_late_count
FROM recon.reconciliation_run;
