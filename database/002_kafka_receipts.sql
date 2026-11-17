-- Run after 001_schema.sql. Existing business data is preserved.
CREATE TABLE IF NOT EXISTS recon.kafka_receipt (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    message_offset BIGINT NOT NULL CHECK (message_offset >= 0),
    event_id TEXT NOT NULL REFERENCES recon.payment_event(event_id),
    payload_hash TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('INSERTED', 'DUPLICATE')),
    consumer_group TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(topic, partition_id, message_offset)
);
