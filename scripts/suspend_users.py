import argparse
from app.db import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from app.db.model import ApiKey


def get_args():
    """
    method for parsing of arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--database", required=True)
    parser.add_argument("--api-keys", nargs='+', help="List of API keys to not suspend."
                                                      "If empty, all API keys are not suspended.")

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = get_args()

    engine = create_engine(f'{args.database}',
                           connect_args={})
    db_session = scoped_session(sessionmaker(autocommit=False,
                                             autoflush=False,
                                             bind=engine))
    Base.query = db_session.query_property()
    Base.metadata.create_all(bind=engine)

    api_keys = db_session.query(ApiKey).all()
    if args.api_keys is None:
        for api_key in api_keys:
            api_key.suspension = False
    else:
        for api_key in api_keys:
            if api_key.api_string in args.api_keys:
                api_key.suspension = False
            else:
                api_key.suspension = True
    db_session.commit()
