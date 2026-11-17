"""Project CLI. Run `python scripts/run.py --help` from the project root."""
import argparse
import json
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    commands.add_parser('generate',help='Write deterministic synthetic event and settlement files')
    commands.add_parser('init',help='Create project schema in the already-created database')
    for name in ('ingest-events','ingest-settlements'):
        p=commands.add_parser(name);p.add_argument('file',type=Path)
    p=commands.add_parser('reconcile');p.add_argument('--as-of',required=True,help='ISO timestamp including timezone')
    commands.add_parser('report',help='Print open exceptions')
    args=parser.parse_args()
    if args.command=='generate':
        from payments.demo import generate
        events,initial,late=generate(ROOT/'data')
        print(f'Generated {len(events)} event lines, {len(initial)} initial settlement rows, and {len(late)} late row.')
        return
    from payments.storage import connect,ingest,run_reconciliation
    if args.command=='init':
        connection=connect()
        try:
            with connection:
                with connection.cursor() as cursor:cursor.execute((ROOT/'database'/'001_schema.sql').read_text())
            print('Created recon schema and tables in the configured business database.')
        finally:connection.close()
    elif args.command.startswith('ingest-'):
        result=ingest(args.file,'events' if args.command=='ingest-events' else 'settlements')
        print(json.dumps(result,indent=2))
    elif args.command=='reconcile':print(json.dumps(run_reconciliation(args.as_of),indent=2))
    elif args.command=='report':
        from psycopg2.extras import RealDictCursor
        connection=connect()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute('SELECT * FROM recon.vw_open_exceptions ORDER BY rule_code,entity_id')
                print(json.dumps(cursor.fetchall(),indent=2,default=str))
        finally:connection.close()


if __name__=='__main__':
    try:main()
    except Exception as error:
        print(f'ERROR: {error}',file=sys.stderr)
        sys.exit(1)
