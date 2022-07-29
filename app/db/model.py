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
    processing_timestamp = Column(DateTime(), nullable=True)
    finish_timestamp = Column(DateTime(), nullable=True, index=True)

    request_id = Column(GUID(), ForeignKey('request.id'), nullable=False, index=True)
    engine_version = Column(String(), nullable=True)

    def __init__(self, name, url, state, request_id):
        self.name = name
        self.url = url
        self.state = state
        self.request_id = request_id

# Pipeline
class Engine(Base):
    __tablename__ = 'engine'
    id = Column(Integer(), primary_key=True)
    name = Column(String(), nullable=False)
    description = Column(String(), nullable=True)
    pipeline = Column(String(), nullable=False)  # list of stages

    def __init__(self, name, description, pipeline):
        self.name = name
        self.description = description
        self.pipeline = pipeline


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

    engine_2 = Engine('Engine_2', 'description')
    db_session.add(engine_2)
    db_session.commit()

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
