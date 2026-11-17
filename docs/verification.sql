-- Run while connected to payments_recon_db.
SELECT COUNT(*) AS unique_events FROM recon.payment_event;
SELECT COUNT(*) AS settlement_rows FROM recon.settlement;
SELECT source_kind, rows_read, rows_inserted, duplicate_rows
FROM recon.ingestion_batch ORDER BY completed_at;

SELECT * FROM recon.vw_run_summary ORDER BY completed_at;
SELECT rule_code, COUNT(*) AS open_exception_count
FROM recon.exception WHERE status='OPEN'
GROUP BY rule_code ORDER BY rule_code;

-- After the late settlement: expect RESOLVED for PAY0003/MISSING_SETTLEMENT.
SELECT entity_id, rule_code, status, explanation, resolved_at
FROM recon.exception WHERE entity_id='PAY0003';

-- View the immutable outcome for PAY0003 on each run.
SELECT r.as_of, d.entity_id, d.status, d.explanation
FROM recon.reconciliation_result d
JOIN recon.reconciliation_run r USING(run_id)
WHERE d.entity_id='PAY0003'
ORDER BY r.completed_at;

-- Affected amounts by currency; these are not confirmed financial losses.
SELECT currency, rule_code, COUNT(*) AS exceptions,
       SUM(amount_minor)/100.0 AS affected_amount_major
FROM recon.exception WHERE status='OPEN'
GROUP BY currency,rule_code ORDER BY currency,rule_code;
