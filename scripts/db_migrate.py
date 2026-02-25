import sys
from sqlalchemy import create_engine
import argparse
from db.base import Base
from db.models import *  # noqa: F401,F403 – ensure all tables are registered


def parseargs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-db', type=str, help="Database.")
    parser.add_argument('--dest-db', type=str, help="Database.")
    args = parser.parse_args()
    return args


def main():
    args = parseargs()

    src = create_engine(args.source_db)

    dst = create_engine(args.dest_db)

    tables = Base.metadata.tables
    table_order = ['model', 'engine', 'engine_version', 'engine_version_model', 'api_key','request', 'page']
    for tbl in table_order:
        print('##################################')
        print(tbl, type(tbl))
        print(tables[tbl].select())
        with src.connect() as src_conn:
            data = src_conn.execute(tables[tbl].select()).fetchall()
        if data:
            with dst.begin() as dst_conn:
                dst_conn.execute(tables[tbl].insert(), [row._mapping for row in data])

    print('done')


if __name__ == '__main__':
    sys.exit(main())



