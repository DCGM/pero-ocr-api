import os
import argparse

from pathlib import Path
from datetime import date
from distutils.dir_util import copy_tree

from app.db import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from app.db.model import Engine, EngineVersion, Model, EngineVersionModel
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

    parser.add_argument("-e", "--engine", default=None,
                        help="Engine ID for existing engine.")
    parser.add_argument("--engine-name", default=None,
                        help="Required for creating new engine, when -e not declared.")
    parser.add_argument("--engine-description", default=None,
                        help="Voluntary for creating new engine, when -e not declared.")
    parser.add_argument("--engine-version-name", default=None,
                        help="Voluntary for creating new engine_version, otherwise current YY-MM-DD is used as a name.")
    parser.add_argument("--engine-version-description", default=None,
                        help="Voluntary for creating new engine_version.")
    parser.add_argument("-d", "--database", required=True)
    parser.add_argument("-m", "--models", required=True, nargs='+',
                        help="List of models for new engine version. Model can be model ID (int) or path to folder "
                             "containing model files and config.ini used as a config in DB. Folder name will be used "
                             "as model name.")

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = get_args()

    engine = args.engine
    engine_name = args.engine_name
    engine_description = args.engine_description

    engine_version_name = args.engine_version_name
    engine_version_description = args.engine_version_description

    models = args.models

    # check validity of models
    if len(models) < 2 and len(models) > 3:
        print("Bad model count.")
        exit(-1)

    for model in models:
        if not model.isdecimal() and not os.path.isdir(model):
            print("Bad model specification.")
            exit(-1)

    # connect DB
    db_engine = create_engine(args.database,
                           convert_unicode=True,
                           connect_args={})
    db_session = scoped_session(sessionmaker(autocommit=False,
                                             autoflush=False,
                                             bind=db_engine))
    Base.query = db_session.query_property()
    Base.metadata.create_all(bind=db_engine)

    # get or create engine
    if engine is None:
        db_engine = Engine(engine_name, engine_description)
        db_session.add(db_engine)
        db_session.commit()
    else:
        db_engine = db_session.query(Engine).filter(Engine.id == engine).first()

    # create new engine version
    if args.engine_version_name is None:
        today = date.today()
        engine_version_name = today.strftime("%Y-%m-%d")
    engine_version = EngineVersion(engine_version_name, db_engine.id, engine_version_description)
    db_session.add(engine_version)

    # create new models
    db_models = []
    for model in models:
        if model.isdecimal():
            db_models.append(db_session.query(Model).filter(Model.id == int(model)).first())
        else:
            name = os.path.basename(os.path.normpath(model))
            config = Path(os.path.join(model, 'config.ini')).read_text()
            db_model = Model(name, config)
            db_session.add(db_model)
            db_models.append(db_model)
            Path(os.path.join(Config.MODELS_FOLDER, name)).mkdir(parents=True, exist_ok=True)
            copy_tree(model, os.path.join(Config.MODELS_FOLDER, name))

    # connect models to engine version
    db_session.commit()
    print(engine_version)
    for model in db_models:
        print(model)
        engine_version_model = EngineVersionModel(engine_version.id, model.id)
        db_session.add(engine_version_model)

    db_session.commit()
