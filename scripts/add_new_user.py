import argparse
from app.db import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from app.db.api_key import generate_hash_key
from app.db.model import ApiKey, Permission


def get_args():
    """
    method for parsing of arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--owner", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--permission", default='USER', choices=['USER', 'SUPER_USER'])

    args = parser.parse_args()

    return args


def add_new_api_key_to_db(db_session, owner, permission):
    api_string = generate_hash_key()
    api_key = ApiKey(api_string, owner, permission)
    db_session.add(api_key)
    db_session.commit()

    return api_string


if __name__ == '__main__':
    args = get_args()

    owner = args.owner
    if args.permission == 'USER':
        permission = Permission.USER
    elif args.permission == 'SUPER_USER':
        permission = Permission.SUPER_USER

    engine = create_engine(f'{args.database}',
                           connect_args={})
    db_session = scoped_session(sessionmaker(autocommit=False,
                                             autoflush=False,
                                             bind=engine))
    Base.query = db_session.query_property()
    Base.metadata.create_all(bind=engine)

    api_string = add_new_api_key_to_db(db_session, owner, permission)
    print(api_string)
