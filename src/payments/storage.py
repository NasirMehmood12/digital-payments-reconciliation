"""PostgreSQL persistence, immutable source identities, and atomic reconciliation runs."""
import csv
import hashlib
import json
import os
import re
import uuid
from pathlib import Path
from datetime import datetime, timezone

from .domain import reconcile, timestamp
from .demo import SETTLEMENT_FIELDS

ROOT = Path(__file__).resolve().parents[2]
LOCK_ID = 739512084
EVENT_FIELDS = ['event_id','transaction_id','event_type','original_payment_id','status','amount_minor','currency','occurred_at']


def connect():
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv(ROOT/'.env', override=False)
    missing=[k for k in ('DB_HOST','DB_NAME','DB_USER','DB_PASSWORD') if not os.getenv(k)]
    if missing:
        raise ValueError('Missing project-root .env settings: '+', '.join(missing))
    return psycopg2.connect(host=os.environ['DB_HOST'],port=int(os.getenv('DB_PORT','5432')),
        dbname=os.environ['DB_NAME'],user=os.environ['DB_USER'],password=os.environ['DB_PASSWORD'],connect_timeout=10)


def fingerprint(row):
    return hashlib.sha256(json.dumps(row,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def validate(row, kind):
    fields=EVENT_FIELDS if kind=='events' else SETTLEMENT_FIELDS
    if not isinstance(row,dict) or set(fields)-set(row):
        raise ValueError('Input is not an object or required columns are missing')
    row={key:row[key] for key in fields}
    ids=['event_id','transaction_id'] if kind=='events' else ['settlement_id','payment_id']
    for key in ids:
        if not isinstance(row[key],str) or not row[key].strip():raise ValueError(f'{key} is blank')
        row[key]=row[key].strip()
    if not isinstance(row['currency'],str) or not re.fullmatch('[A-Z]{3}',row['currency']):
        raise ValueError('currency must contain 3 uppercase letters')
    if row['currency'] not in ('PKR','USD'):
        raise ValueError('This demo supports only PKR and USD, both with 100 minor units per major unit')
    amounts=['amount_minor'] if kind=='events' else ['gross_minor','fee_minor','net_minor']
    for key in amounts:
        value=row[key]
        if isinstance(value,bool) or not re.fullmatch(r'\d+',str(value)):raise ValueError(f'{key} must be an integer')
        value=int(value)
        if value>=2**63 or value<0 or (kind=='events' and value==0):raise ValueError(f'{key} is out of range')
        row[key]=value
    time_key='occurred_at' if kind=='events' else 'settled_at'
    row[time_key]=timestamp(row[time_key]).isoformat()
    if kind=='events':
        if row['event_type'] not in ('PAYMENT','REFUND') or row['status'] not in ('SUCCESS','FAILED'):
            raise ValueError('Invalid event_type or status')
        parent=row['original_payment_id']
        if row['event_type']=='REFUND':
            if not isinstance(parent,str) or not parent.strip():raise ValueError('Refund requires original_payment_id')
            row['original_payment_id']=parent.strip()
        elif parent is not None:raise ValueError('Payment original_payment_id must be null')
    return row


def read_source(path,kind):
    raw=path.read_bytes()
    batch=f'{kind}:'+hashlib.sha256(raw).hexdigest()
    rows,errors=[],[]
    if kind=='events':
        for number,line in enumerate(raw.decode('utf-8-sig').splitlines(),1):
            if not line.strip():continue
            try:rows.append(validate(json.loads(line),kind))
            except (ValueError,TypeError,AttributeError) as error:
                errors.append({'source_line':number,'reason':str(error)})
    else:
        with path.open(encoding='utf-8-sig',newline='') as f:
            reader=csv.DictReader(f)
            if not reader.fieldnames or set(SETTLEMENT_FIELDS)-set(reader.fieldnames):
                raise ValueError('Settlement CSV required columns are missing')
            if len(reader.fieldnames)!=len(set(reader.fieldnames)):
                raise ValueError('Settlement CSV has duplicate headers')
            for number,row in enumerate(reader,2):
                try:
                    if None in row:raise ValueError('CSV row has extra values')
                    rows.append(validate(row,kind))
                except (ValueError,TypeError,AttributeError) as error:
                    errors.append({'source_line':number,'reason':str(error)})
    if errors:
        destination=ROOT/'data'/'quarantine';destination.mkdir(parents=True,exist_ok=True)
        file=destination/(path.stem+'_'+batch.split(':')[1][:12]+'_errors.json')
        file.write_text(json.dumps(errors,indent=2),encoding='utf-8')
        raise ValueError(f'{len(errors)} invalid rows. Whole file rejected; reasons saved to {file}')
    if not rows:raise ValueError('Input contains no data rows')
    return batch,rows


def ingest(path,kind):
    from psycopg2.extras import Json
    path=Path(path)
    batch,rows=read_source(path,kind)
    connection=connect()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT pg_advisory_xact_lock(%s)',(LOCK_ID,))
                cursor.execute('SELECT 1 FROM recon.ingestion_batch WHERE batch_id=%s',(batch,))
                if cursor.fetchone():return {'status':'SKIPPED','file':path.name,'reason':'Identical successful source batch'}
                inserted=duplicates=0
                for row in rows:
                    digest=fingerprint(row)
                    if kind=='events':
                        cursor.execute('SELECT payload_hash FROM recon.payment_event WHERE event_id=%s',(row['event_id'],))
                    else:
                        cursor.execute('SELECT payload_hash FROM recon.settlement WHERE settlement_id=%s',(row['settlement_id'],))
                    previous=cursor.fetchone()
                    if previous:
                        if previous[0]!=digest:
                            raise ValueError('An existing source ID was reused with different contents; batch rolled back')
                        duplicates+=1
                        continue
                    if kind=='events':
                        cursor.execute('''INSERT INTO recon.payment_event
                            (event_id,transaction_id,event_type,original_payment_id,status,amount_minor,currency,
                             occurred_at,payload,payload_hash,source_batch_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                            tuple(row[k] for k in EVENT_FIELDS)+(Json(row),digest,batch))
                    else:
                        cursor.execute('''INSERT INTO recon.settlement
                            (settlement_id,payment_id,gross_minor,fee_minor,net_minor,currency,settled_at,payload_hash,source_batch_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)''',tuple(row[k] for k in SETTLEMENT_FIELDS)+(digest,batch))
                    inserted+=1
                cursor.execute('''INSERT INTO recon.ingestion_batch
                    (batch_id,source_kind,source_file,rows_read,rows_inserted,duplicate_rows)
                    VALUES (%s,%s,%s,%s,%s,%s)''',(batch,kind,path.name,len(rows),inserted,duplicates))
        return dict(status='SUCCEEDED',file=path.name,rows_read=len(rows),inserted=inserted,duplicate_rows=duplicates)
    finally:connection.close()


def run_reconciliation(as_of):
    from psycopg2.extras import RealDictCursor,execute_values
    as_of=timestamp(as_of)
    connection=connect()
    try:
        with connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute('SELECT pg_advisory_xact_lock(%s)',(LOCK_ID,))
                cursor.execute('SELECT MAX(as_of) AS latest FROM recon.reconciliation_run')
                latest=cursor.fetchone()['latest']
                if latest and as_of<latest:
                    raise ValueError('as-of cannot be earlier than the last completed run; historical runs would overwrite current exception state')
                cursor.execute('SELECT * FROM recon.payment_event WHERE occurred_at<=%s',(as_of,))
                events=cursor.fetchall()
                if not events:raise ValueError('No payment events found at this as-of time. Ingest the source events first.')
                cursor.execute('SELECT * FROM recon.settlement WHERE settled_at<=%s',(as_of,))
                settlements=cursor.fetchall()
                findings=reconcile(events,settlements,as_of)
                exceptions=[f for f in findings if f.status=='EXCEPTION']
                run_id=str(uuid.uuid4())
                matched=sum(f.status in ('MATCHED','MATCHED_LATE') for f in findings)
                late=sum(f.status=='MATCHED_LATE' for f in findings)
                cursor.execute('''INSERT INTO recon.reconciliation_run
                    (run_id,as_of,findings_count,exception_count,matched_payment_count,matched_late_count)
                    VALUES (%s,%s,%s,%s,%s,%s)''',(run_id,as_of,len(findings),len(exceptions),matched,late))
                if findings:
                    execute_values(cursor,'''INSERT INTO recon.reconciliation_result
                        (run_id,finding_key,entity_id,rule_code,status,explanation,amount_minor,currency) VALUES %s''',
                        [(run_id,f.key,f.entity_id,f.rule_code,f.status,f.explanation,f.amount_minor,f.currency) for f in findings])
                active=[f.key for f in exceptions]
                cursor.execute('''UPDATE recon.exception SET status='RESOLVED',resolved_at=NOW(),updated_at=NOW(),last_run_id=%s
                    WHERE status='OPEN' AND left(finding_key, 8) = 'DEMO_V1:' AND NOT (finding_key=ANY(%s::text[]))''',(run_id,active))
                resolved=cursor.rowcount
                for finding in exceptions:
                    cursor.execute('''INSERT INTO recon.exception
                        (finding_key,entity_id,rule_code,status,explanation,amount_minor,currency,last_run_id)
                        VALUES (%s,%s,%s,'OPEN',%s,%s,%s,%s)
                        ON CONFLICT(finding_key) DO UPDATE SET status='OPEN',explanation=EXCLUDED.explanation,
                            amount_minor=EXCLUDED.amount_minor,currency=EXCLUDED.currency,last_run_id=EXCLUDED.last_run_id,
                            updated_at=NOW(),resolved_at=NULL,
                            opened_at=CASE WHEN recon.exception.status='RESOLVED' THEN NOW() ELSE recon.exception.opened_at END''',
                        (finding.key,finding.entity_id,finding.rule_code,finding.explanation,finding.amount_minor,finding.currency,run_id))
        return dict(run_id=run_id,as_of=as_of.isoformat(),findings=len(findings),open_exceptions=len(exceptions),
                    resolved_this_run=resolved,matched_payments=matched,matched_late=late)
    finally:connection.close()
