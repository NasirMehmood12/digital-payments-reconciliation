"""Local producer/consumer CLI. Run from the project root."""
import argparse
import json
import logging
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
LOG=logging.getLogger('payments_stream')
TOPIC='payments.events.v1'
GROUP='payments-postgres-v1'


def init(bootstrap,topic):
    from kafka.admin import KafkaAdminClient,NewTopic
    from payments.storage import connect
    # An additive migration; does not reset or truncate any existing data.
    connection=connect()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute((ROOT/'database'/'002_kafka_receipts.sql').read_text())
    finally:connection.close()
    admin=KafkaAdminClient(bootstrap_servers=bootstrap,client_id='payments-admin')
    try:
        if topic not in admin.list_topics():
            admin.create_topics([NewTopic(name=topic,num_partitions=3,replication_factor=1,
                                          topic_configs={'retention.ms':'604800000'})])
        LOG.info('Database receipt table and topic %s are ready.',topic)
    finally:admin.close()


def produce(bootstrap,topic,path):
    from kafka import KafkaProducer
    from payments.storage import read_source
    _,rows=read_source(path,'events')
    producer=KafkaProducer(bootstrap_servers=bootstrap,client_id='payments-producer',
        acks='all',retries=5,max_in_flight_requests_per_connection=1,
        key_serializer=lambda value:value.encode('utf-8'),
        value_serializer=lambda value:json.dumps(value,sort_keys=True).encode('utf-8'))
    try:
        for row in rows:
            producer.send(topic,key=row['transaction_id'],value=row).get(timeout=30)
        LOG.info('Published %s messages to %s. Re-publishing is allowed; PostgreSQL deduplicates event IDs.',len(rows),topic)
    finally:producer.close(timeout=30)


def consume(bootstrap,topic,group,idle_seconds,crash):
    from kafka import KafkaConsumer
    from kafka.structs import TopicPartition,OffsetAndMetadata
    from payments.streaming import process_message,persist_event
    consumer=KafkaConsumer(topic,bootstrap_servers=bootstrap,group_id=group,
        auto_offset_reset='earliest',enable_auto_commit=False,
        consumer_timeout_ms=idle_seconds*1000,max_poll_records=1,
        max_poll_interval_ms=300000,client_id='payments-postgres-consumer')
    count=0
    LOG.info('Group=%s; will exit after %ss without messages. Ctrl+C stops safely.',group,idle_seconds)
    try:
        for record in consumer:
            coordinates=dict(topic=record.topic,partition=record.partition,offset=record.offset,group=group)
            tp=TopicPartition(record.topic,record.partition)
            def acknowledge(next_offset):
                consumer.commit({tp:OffsetAndMetadata(next_offset,'',-1)})
            outcome=process_message(record.value,coordinates,persist_event,acknowledge,crash_after_db=crash)
            count+=1
            LOG.info('%s | partition=%s offset=%s | acknowledged=%s',outcome,record.partition,record.offset,record.offset+1)
        LOG.info('Consumer finished. %s messages handled.',count)
    finally:
        # Do not acknowledge work on failure, interruption, or the simulated crash.
        consumer.close(autocommit=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bootstrap',default='127.0.0.1:29092')
    parser.add_argument('--topic',default=TOPIC)
    commands=parser.add_subparsers(dest='command',required=True)
    commands.add_parser('init')
    p=commands.add_parser('produce');p.add_argument('--file',type=Path,default=ROOT/'data/events/payments.jsonl')
    c=commands.add_parser('consume');c.add_argument('--group',default=GROUP)
    c.add_argument('--idle-seconds',type=int,default=15)
    c.add_argument('--crash-after-db',action='store_true',help='Exit with code 9 after first database commit but before Kafka offset acknowledgement')
    args=parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s | %(message)s')
    logging.getLogger('kafka').setLevel(logging.WARNING)
    if args.command=='init':init(args.bootstrap,args.topic)
    elif args.command=='produce':produce(args.bootstrap,args.topic,args.file)
    else:
        if args.idle_seconds<1:parser.error('--idle-seconds must be positive')
        consume(args.bootstrap,args.topic,args.group,args.idle_seconds,args.crash_after_db)


if __name__=='__main__':
    try:main()
    except KeyboardInterrupt:print('Consumer stopped; restart with the same group to resume.')
    except Exception as error:
        print(f'ERROR: {error}',file=sys.stderr)
        sys.exit(1)
