import os
import sys
import argparse

from pathlib import Path
from datetime import date
from distutils.dir_util import copy_tree

from app.db import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from app.db.model import Engine
from config import Config


def get_args():
    """
    CALL EXAMPLES:

    adds new engine version for engine with ID 3, containing model with ID 1 and new model located in path
    python3 add_new_model.py --engine 3 -m 1 /mnt/c/ocr_2020-11-20 -d /mnt/c/database.db

    adds new engine version for new engine called great_ocr, containing models with IDs 1 and 2
    python3 add_new_model.py --engine_name great_ocr -m 1 2 -d /mnt/c/database.db
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-e", "--engine",
        help="Engine ID for existing engine.",
        default=None
    )
    parser.add_argument(
        "-n", "--engine_name",
        help="Required for creating new engine, when -e not declared.",
        default=None
    )
    parser.add_argument(
        "-s", "--engine_description",
        help="Voluntary for creating new engine, when -e not declared.",
        default=None
    )
    parser.add_argument(
        "-t", "--engine_stages",
        help="Processing pipeline stages for given engine. Required for creating new engine.",
        nargs='+',
        default=[]
    )
    parser.add_argument(
        "-d", "--database",
        help="URL of database to connect to. Required argument!",
        required=True
    )

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = get_args()

    engine = args.engine
    engine_name = args.engine_name
    engine_description = args.engine_description
    engine_pipeline = ','.join(args.engine_stages)

    if not engine:
        if not engine_pipeline:
            raise ValueError('Engine stages are required for creating new engine!')
        if not engine_name:
            raise ValueError('Engine name required for creating new engine!')
    
    if engine and not engine_name and not engine_description and not engine_pipeline:
        sys.exit(0)

    # connect DB
    db_engine = create_engine(f'{args.database}',
                           connect_args={})
    db_session = scoped_session(sessionmaker(autocommit=False,
                                             autoflush=False,
                                             bind=db_engine))
    Base.query = db_session.query_property()
    Base.metadata.create_all(bind=db_engine)

    # get or create engine
    if engine is None:
        new_engine = Engine(engine_name, engine_description, engine_pipeline)
        db_session.add(new_engine)
    else:
        new_engine = db_session.query(Engine).filter(Engine.id == engine).first()
        if not new_engine:
            raise ValueError(f'No engine with ID {engine}!')
        if engine_name:
            new_engine.name = engine_name
        if engine_description:
            new_engine.description = engine_description
        if engine_pipeline:
            new_engine.pipeline = engine_pipeline
    db_session.commit()
