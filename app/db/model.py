import enum
import uuid
import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, relationship
from sqlalchemy import Column, Enum, ForeignKey, Integer, String, DateTime, Float, Boolean
from app.db import Base

from app.db.guid import GUID


class PageState(enum.Enum):
    CREATED = 'Page was created.'
    WAITING = 'Page is waiting for processing.'
    PROCESSING = 'Page is being processed.'
    NOT_FOUND = 'Page image was not found.'
    INVALID_FILE = 'Page image is invalid.'
    PROCESSING_FAILED = 'Page processing failed.'
    PROCESSED = 'Page was processed.'
    CANCELED = 'Page processing was canceled.'
    EXPIRED = 'Page expired.'


class Permission(enum.Enum):
    SUPER_USER = 'User can take and process requests.'
    USER = 'User can create requests.'


class ApiKey(Base):
    __tablename__ = 'api_key'
    id = Column(Integer(), primary_key=True)
    api_string = Column(String(), nullable=False, index=True)
    owner = Column(String(), nullable=False)
    permission = Column(Enum(Permission), nullable=False)
    suspension = Column(Boolean(), nullable=False, default=False)
    priority = Column(Integer(), nullable=False, default=1)

    def __init__(self, api_string, owner, permission):
        self.api_string = api_string
        self.owner = owner
        self.permission = permission


class Request(Base):
    __tablename__ = 'request'
    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    creation_timestamp = Column(DateTime(), nullable=False, index=True, default=datetime.datetime.utcnow)
    modification_timestamp = Column(DateTime(), nullable=False, index=True, default=datetime.datetime.utcnow)
    finish_timestamp = Column(DateTime(), nullable=True, index=True)

    engine_id = Column(Integer(), ForeignKey('engine.id'), nullable=False)
    api_key_id = Column(Integer(), ForeignKey('api_key.id'), nullable=False)

    def __init__(self, engine_id, api_key_id):
        self.engine_id = engine_id
        self.api_key_id = api_key_id


class Page(Base):
    __tablename__ = 'page'
    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    name = Column(String(), nullable=False, index=True)
    url = Column(String(), nullable=True)
    state = Column(Enum(PageState), nullable=False, index=True)
    score = Column(Float(), nullable=True, index=True)
    traceback = Column(String(), nullable=True)
    waiting_timestamp = Column(DateTime(), nullable=True, index=True)
    processing_timestamp = Column(DateTime(), nullable=True)
    finish_timestamp = Column(DateTime(), nullable=True, index=True)

    request_id = Column(GUID(), ForeignKey('request.id'), nullable=False, index=True)
    engine_version = Column(Integer(), ForeignKey('engine_version.id'), nullable=True, index=True)

    def __init__(self, name, url, state, request_id, waiting_timestamp=None):
        self.name = name
        self.url = url
        self.state = state
        self.request_id = request_id
        self.waiting_timestamp = waiting_timestamp


class Engine(Base):
    __tablename__ = 'engine'
    id = Column(Integer(), primary_key=True)
    name = Column(String(), nullable=False)
    description = Column(String(), nullable=True)

    def __init__(self, name, description):
        self.name = name
        self.description = description


class EngineVersion(Base):
    __tablename__ = 'engine_version'
    id = Column(Integer(), primary_key=True)
    version = Column(String(), nullable=False)
    description = Column(String(), nullable=True)
    engine_id = Column(Integer(), ForeignKey('engine.id'), nullable=False)

    def __init__(self, version, engine_id, description=None):
        self.version = version
        self.engine_id = engine_id
        self.description = description


class EngineVersionModel(Base):
    __tablename__ = 'engine_version_model'
    id = Column(Integer(), primary_key=True)
    engine_version_id = Column(Integer(), ForeignKey('engine_version.id'), nullable=False)
    model_id = Column(Integer(), ForeignKey('model.id'), nullable=False)

    def __init__(self, engine_version_id, model_id):
        self.engine_version_id = engine_version_id
        self.model_id = model_id


class Model(Base):
    __tablename__ = 'model'
    id = Column(Integer(), primary_key=True)
    name = Column(String(), nullable=False)
    config = Column(String(), nullable=False)

    def __init__(self, name, config):
        self.name = name
        self.config = config


class Notification(Base):
    __tablename__ = 'notification'
    id = Column(Integer(), primary_key=True)
    last_notification = Column(DateTime(), nullable=False)

    def __init__(self, last_notification):
        self.last_notification = last_notification


if __name__ == '__main__':
    engine = create_engine('sqlite:///{}'.format('C:/Users/LachubCz_NTB/Documents/GitHub/PERO-API/app/database.db'),
                           convert_unicode=True,
                           connect_args={'check_same_thread': False})
    db_session = scoped_session(sessionmaker(autocommit=False,
                                             autoflush=False,
                                             bind=engine))
    Base.query = db_session.query_property()
    Base.metadata.create_all(bind=engine)

    pages = db_session.query(Page).all()

    engine_1 = Engine('Engine_1', 'description')
    db_session.add(engine_1)
    db_session.commit()

    engine_version_1_1 = EngineVersion('v0.0.1', engine_1.id)
    db_session.add(engine_version_1_1)
    db_session.commit()

    engine_2 = Engine('Engine_2', 'description')
    db_session.add(engine_2)
    db_session.commit()

    engine_version_2_1 = EngineVersion('v0.0.1', engine_2.id)
    db_session.add(engine_version_2_1)
    db_session.commit()

    engine_version_1_2 = EngineVersion('v0.0.2', engine_1.id)
    db_session.add(engine_version_1_2)
    db_session.commit()

    model_1 = Model('lidove_noviny',
                    ('[LINE_CROPPER]\n'
                     'INTERP = 2\n'
                     'LINE_SCALE = 1\n'
                     'LINE_HEIGHT = 40\n'
                     '\n'
                     '[OCR]\n'
                     'METHOD = pytorch_ocr\n'
                     'OCR_JSON = ./lidove_noviny/ocr_engine.json\n'))
    db_session.add(model_1)
    db_session.commit()

    model_2 = Model('universal',
                    ('[LAYOUT_PARSER]\n'
                    'METHOD = LAYOUT_CNN\n'
                    'MODEL_PATH = ./universal/ParseNet_exported\n'
                    'USE_CPU = yes\n'
                    '\n'
                    'DETECT_LINES = yes\n'
                    'DETECT_REGIONS = no\n'
                    'MERGE_LINES = no\n'
                    'ADJUST_HEIGHTS = no\n'
                    '\n'
                    'MAX_MEGAPIXELS = 5\n'
                    'GPU_FRACTION = 0.5\n'
                    'DOWNSAMPLE = 4\n'
                    'PAD = 52\n'
                    'DETECTION_THRESHOLD = 0.2\n'))
    db_session.add(model_2)
    db_session.commit()

    engine_version_model_1 = EngineVersionModel(engine_version_1_1.id, model_1.id)
    db_session.add(engine_version_model_1)
    engine_version_model_2 = EngineVersionModel(engine_version_1_1.id, model_2.id)
    db_session.add(engine_version_model_2)

    engine_version_model_3 = EngineVersionModel(engine_version_2_1.id, model_1.id)
    db_session.add(engine_version_model_3)
    engine_version_model_4 = EngineVersionModel(engine_version_2_1.id, model_2.id)
    db_session.add(engine_version_model_4)

    engine_version_model_5 = EngineVersionModel(engine_version_1_2.id, model_1.id)
    db_session.add(engine_version_model_5)
    engine_version_model_6 = EngineVersionModel(engine_version_1_2.id, model_2.id)
    db_session.add(engine_version_model_6)

    api_key = ApiKey('test_user', 'Owner of The Key', Permission.SUPER_USER)
    db_session.add(api_key)
    db_session.commit()

    request = Request(engine_1.id, api_key.id)
    db_session.add(request)
    db_session.commit()

    page1 = Page('Magna_Carta', 'https://upload.wikimedia.org/wikipedia/commons/e/ee/Magna_Carta_%28Brwitish_Library_Cotton_MS_Augustus_II.106%29.jpg', request.id)
    db_session.add(page1)
    page2 = Page('United_States_Declaration_of_Independence', 'https://upload.wikimedia.org/wikipedia/commons/8/8f/Unitedw_States_Declaration_of_Independence.jpg', request.id)
    db_session.add(page2)
    db_session.commit()

    page1.state = PageState.PROCESSED
    page1.score = 86.7
    db_session.commit()

    request = Request(engine_2.id, api_key.id)
    db_session.add(request)
    db_session.commit()

    page1 = Page('Magna_Carta', 'https://raw.githubusercontent.com/LachubCz/PERO-API/d7c6442c455c83ed2d84141f1caef50d18064e1d/processing_client/engines/example_config.ini?token=AELVMCLUF2E224H2U6FLJD27Q4MGE', request.id)
    db_session.add(page1)
    page2 = Page('United_States_Declaration_of_Independence', 'https://upload.wikimedia.org/wikipedia/commons/8/8f/United_States_Declaration_of_Independence.jpg', request.id)
    db_session.add(page2)
    db_session.commit()
