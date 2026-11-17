"""At-least-once stream handling with idempotent PostgreSQL writes."""
import json
import logging
from .storage import connect,validate,fingerprint,EVENT_FIELDS,LOCK_ID


def process_message(raw_value, coordinates, persist, acknowledge, crash_after_db=False):
    """The acknowledgement callback MUST follow successful durable persistence."""
    event=validate(json.loads(raw_value.decode('utf-8')), 'events')
    result=persist(event,coordinates)
    if crash_after_db:
        logging.getLogger('payments_stream').warning('Simulated stop: database committed; Kafka offset intentionally not acknowledged. Restart without --crash-after-db.')
        raise SystemExit(9)  # Deliberate demo: DB committed, Kafka offset not committed.
    acknowledge(coordinates['offset']+1)
    return result


def persist_event(event,coordinates):
    from psycopg2.extras import Json
    digest=fingerprint(event)
    position=(coordinates['topic'],coordinates['partition'],coordinates['offset'])
    source=f'kafka:{position[0]}:{position[1]}:{position[2]}'
    connection=connect()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT pg_advisory_xact_lock(%s)',(LOCK_ID,))
                cursor.execute('''SELECT payload_hash FROM recon.kafka_receipt
                    WHERE topic=%s AND partition_id=%s AND message_offset=%s''',position)
                receipt=cursor.fetchone()
                if receipt:
                    if receipt[0]!=digest:
                        raise ValueError('Kafka coordinate reused with different contents; preserve broker volumes and investigate')
                    return 'DB_REPLAY'
                cursor.execute('SELECT payload_hash FROM recon.payment_event WHERE event_id=%s',(event['event_id'],))
                existing=cursor.fetchone()
                if existing and existing[0]!=digest:
                    raise ValueError('Existing event_id has different contents; no offset acknowledged')
                outcome='DUPLICATE' if existing else 'INSERTED'
                if not existing:
                    cursor.execute('''INSERT INTO recon.payment_event
                        (event_id,transaction_id,event_type,original_payment_id,status,amount_minor,currency,
                         occurred_at,payload,payload_hash,source_batch_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                        tuple(event[k] for k in EVENT_FIELDS)+(Json(event),digest,source))
                cursor.execute('''INSERT INTO recon.kafka_receipt
                    (topic,partition_id,message_offset,event_id,payload_hash,outcome,consumer_group)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)''',
                    position+(event['event_id'],digest,outcome,coordinates['group']))
        return outcome
    finally:connection.close()
