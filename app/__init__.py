import os
import shutil
import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from flask import Flask
from flask_bootstrap import Bootstrap
from flask_jsglue import JSGlue
from flask_dropzone import Dropzone
from flask_sqlalchemy_session import flask_scoped_session

from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler

from config import *
from .db import Base, Page, PageState, Request, Notification, ApiKey, Engine
from app.mail.mail import send_mail


engine = create_engine(database_url, convert_unicode=True)
session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db_session = init_db(app)

    scheduler = BackgroundScheduler()
    scheduler.start()
    #scheduler.add_job(processing_timeout, 'interval', seconds=60)  # TODO
    scheduler.add_job(old_files_removals, 'interval', hours=24)

    Path(app.config['PROCESSED_REQUESTS_FOLDER']).mkdir(parents=True, exist_ok=True)
    Path(app.config['MODELS_FOLDER']).mkdir(parents=True, exist_ok=True)
    Path(app.config['UPLOAD_IMAGES_FOLDER']).mkdir(parents=True, exist_ok=True)

    Bootstrap(app)
    Dropzone(app)  # TODO - vyzkoušet + zrušit pokud je to blbost (web stránka aplikace)

    notification = db_session.query(Notification).first()
    if notification is not None:
        notification.last_notification = datetime.datetime(1970, 1, 1)
    else:
        notification = Notification(datetime.datetime(1970, 1, 1))
        db_session.add(notification)

    db_session.commit()

    jsglue = JSGlue()
    jsglue.init_app(app)

    from app.main import bp as main_bp
    app.register_blueprint(main_bp)

    return app


def init_db(app):
    from app.db import Base
    Base.metadata.create_all(bind=engine)

    db_session = flask_scoped_session(session_factory, app)
    Base.query = db_session.query_property()
    return db_session


def processing_timeout():
    db_session = session_factory()
    try:
        now = datetime.datetime.now()
        delta = datetime.timedelta(seconds=60)
        timestamp = now - delta
    
        pages = db_session.query(Page).filter(Page.state == PageState.PROCESSING).filter(Page.processing_timestamp < timestamp).all()
        message_body = ""
        for page in pages:
            page.state = PageState.WAITING
            page.processing_timestamp = None
    
            request = db_session.query(Request).filter(Request.id == page.request_id).first()
            engine = db_session.query(Engine).filter(Engine.id == request.engine_id).first()
            api_key = db_session.query(ApiKey).filter(ApiKey.id == request.api_key_id).first()
    
            message_body += "owner_api_key: {}<br>" \
                            "owner_description: {}<br>" \
                            "engine_id: {}<br>" \
                            "engine_name: {}<br>" \
                            "request_id: {}<br>" \
                            "page_id: {}<br>" \
                            "page_name: {}<br>" \
                            "page_url: {}<br><br>" \
                            "####################<br><br>" \
                            .format(api_key.api_string,
                                    api_key.owner,
                                    engine.id,
                                    engine.name,
                                    request.id,
                                    page.id,
                                    page.name,
                                    page.url)

        if pages != [] and Config.EMAIL_NOTIFICATION_ADDRESSES != []:
            send_mail(subject="API Bot - PROCESSING TIMEOUT",
                      body=message_body,
                      sender=('PERO OCR - API BOT', Config.MAIL_USERNAME),
                      recipients=Config.EMAIL_NOTIFICATION_ADDRESSES,
                      host=Config.MAIL_SERVER,
                      password=Config.MAIL_PASSWORD)

        db_session.commit()
    except:
        db_session.rollback()
        raise
    finally:
        db_session.close()


def old_files_removals():
    try: 
        now = datetime.datetime.now()
        delta = datetime.timedelta(days=7)
        timestamp = now - delta

        db_session = session_factory()
        pages = db_session.query(Page).outerjoin(Request)\
                          .filter(Request.finish_timestamp < timestamp) \
                          .filter(Page.state.in_([PageState.PROCESSED, PageState.CANCELED])) \
                          .all()
        for page in pages:
            page.state = PageState.EXPIRED

        db_session.commit()

        requests = db_session.query(Request).filter(Request.finish_timestamp < timestamp).all()

        for request in requests:
            requests_dir_path = os.path.join(Config.PROCESSED_REQUESTS_FOLDER, str(request.id))
            images_dir_path = os.path.join(Config.UPLOAD_IMAGES_FOLDER, str(request.id))
            if os.path.isdir(requests_dir_path):
                shutil.rmtree(requests_dir_path)
            if os.path.isdir(images_dir_path):
                shutil.rmtree(images_dir_path)
    except:
        db_session.rollback()
        raise
    finally:
        db_session.close()
