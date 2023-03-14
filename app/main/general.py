import os
import datetime
import sqlalchemy
from sqlalchemy import func
from collections import defaultdict

from app.db.model import Request, Engine, Page, PageState, ApiKey, EngineVersion, Model, EngineVersionModel, \
                         PageState, Notification
from flask_sqlalchemy_session import current_session as db_session
from flask import current_app as app


def request_exists(request_id):
    try:
        request = db_session.query(Request).filter(Request.id == request_id).first()
    except sqlalchemy.exc.StatementError:
        return None

    if request is not None:
        return request
    else:
        return None


def create_request(api_string, json_request):
    engine_id = int(json_request["engine"])
    engine = db_session.query(Engine).filter(Engine.id == engine_id).first()
    api_key = db_session.query(ApiKey).filter(ApiKey.api_string == api_string).first()
    if engine is not None:
        request = Request(engine.id, api_key.id)
        db_session.add(request)
        db_session.commit()
        for image_name in json_request["images"]:
            if json_request["images"][image_name] is None:
                page = Page(image_name, None, PageState.CREATED, request.id)
            else:
                page = Page(image_name, json_request["images"][image_name], PageState.WAITING, request.id,
                            waiting_timestamp=datetime.datetime.utcnow())
            db_session.add(page)
        db_session.commit()
        return request, engine_id
    return None, engine_id


def get_document_status(request_id):
    all = db_session.query(Page).filter(Page.request_id == request_id).count()
    not_processed = db_session.query(Page).filter(Page.request_id == request_id).filter(Page.state.in_([PageState.CREATED, PageState.WAITING, PageState.PROCESSING])).count()
    status = (all - not_processed) / all

    quality = db_session.query(func.avg(Page.score)).filter(Page.request_id == request_id).filter(Page.state == PageState.PROCESSED).first()[0]

    return status, quality


def cancel_request_by_id(request_id):
    waiting_pages = db_session.query(Page).filter(Page.request_id == request_id) \
                                          .filter(Page.state.in_([PageState.CREATED, PageState.WAITING, PageState.PROCESSING]))\
                                          .all()

    timestamp = datetime.datetime.utcnow()
    for page in waiting_pages:
        page.state = PageState.CANCELED
        page.finish_timestamp = timestamp
    db_session.commit()


def get_engine_dict():
    engines = db_session.query(Engine).all()
    engines_dict = dict()
    for engine in engines:
        engine_version, models = get_latest_models(engine.id)
        engines_dict[engine.name] = {'id': engine.id, 'description': engine.description, 'engine_version': engine_version.version, 'models': [{'id': model.id, 'name': model.name} for model in models]}

    return engines_dict


def get_page_by_id(page_id):
    page = db_session.query(Page).filter(Page.id == page_id).first()

    return page


def check_save_path(request_id):
    path = os.path.join(app.config['PROCESSED_REQUESTS_FOLDER'], str(request_id))
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def get_page_by_preferred_engine(engine_id):
    # Get page for preferred engine
    page = None
    if engine_id is not None:
        page = db_session.query(Page).join(Request).join(ApiKey)\
            .filter(Page.state == PageState.WAITING)\
            .filter(Request.engine_id == engine_id)\
            .filter(ApiKey.suspension == False) \
            .order_by(ApiKey.priority.desc(), Page.waiting_timestamp.asc())\
            .first()

    if not page:
        # Get page for any engine
        page = db_session.query(Page).join(Request).join(ApiKey).filter(ApiKey.suspension == False)\
               .filter(Page.state == PageState.WAITING)\
               .order_by(ApiKey.priority.desc(), Page.waiting_timestamp.asc())\
               .first()

        # get engine for the page
        if page:
            engine_id = db_session.query(Request.engine_id).filter(Request.id == page.request_id).first()[0]

    if page:
        page.state = PageState.PROCESSING
        page.processing_timestamp = datetime.datetime.now()
        db_session.commit()

    return page, engine_id


def request_belongs_to_api_key(api_key, request_id):
    api_key = db_session.query(ApiKey).filter(ApiKey.api_string == api_key).first()
    request = db_session.query(Request).filter(Request.api_key_id == api_key.id).filter(Request.id == request_id).first()
    return request


def get_engine_version(engine_id, version_name):
    engine_version = db_session.query(EngineVersion)\
        .filter(EngineVersion.version == version_name)\
        .filter(EngineVersion.engine_id == engine_id)\
        .first()

    return engine_version


def get_engine_by_page_id(page_id):
    page = db_session.query(Page).filter(Page.id == page_id).first()
    request = db_session.query(Request).filter(Request.id == page.request_id).first()
    engine = db_session.query(Engine).filter(Engine.id == request.engine_id).first()

    return engine


def get_usage_statistics(api_string, from_datetime=None, to_datetime=None):
    pages = db_session.query(func.count(Page.id)).filter(Page.state.in_(['PROCESSED', 'EXPIRED'])).join(Request).join(ApiKey).filter(ApiKey.api_string == api_string)
    if from_datetime:
        pages = pages.filter(Page.finish_timestamp >= from_datetime)
    if to_datetime:
        pages = pages.filter(Page.finish_timestamp <= to_datetime)

    return pages.first()[0]


def get_page_statistics(history_hours=24):
    from_datetime = datetime.datetime.utcnow() - datetime.timedelta(hours=history_hours)
    finished_pages = db_session.query(Page).filter(Page.finish_timestamp > from_datetime).all()
    unfinished_pages = db_session.query(Page).filter(Page.finish_timestamp == None).all()
    state_stats = {state.name: 0 for state in PageState if state != PageState.CREATED}

    for page_db in finished_pages:
        state_stats[page_db.state.name] += 1
    for page_db in unfinished_pages:
        if page_db.state == PageState.WAITING or page_db.state == PageState.PROCESSING:
            state_stats[page_db.state.name] += 1

    return state_stats


def change_page_to_processed(page_id, score, engine_version):
    page = db_session.query(Page).filter(Page.id == page_id).first()
    request = db_session.query(Request).filter(Request.id == page.request_id).first()

    page.score = score
    page.state = PageState.PROCESSED
    page.engine_version = engine_version

    timestamp = datetime.datetime.utcnow()
    page.finish_timestamp = timestamp
    request.modification_timestamp = timestamp
    db_session.commit()
    if is_request_processed(request.id):
        request.finish_timestamp = timestamp
        db_session.commit()


def change_page_to_failed(page_id, fail_type, traceback, engine_version):
    page = db_session.query(Page).filter(Page.id == page_id).first()
    request = db_session.query(Request).filter(Request.id == page.request_id).first()

    if fail_type == 'NOT_FOUND':
        page.state = PageState.NOT_FOUND
    elif fail_type == 'INVALID_FILE':
        page.state = PageState.INVALID_FILE
    elif fail_type == 'PROCESSING_FAILED':
        page.state = PageState.PROCESSING_FAILED
    page.traceback = traceback
    page.engine_version = engine_version

    timestamp = datetime.datetime.utcnow()
    page.finish_timestamp = timestamp
    request.modification_timestamp = timestamp
    db_session.commit()
    if is_request_processed(request.id):
        request.finish_timestamp = timestamp
        db_session.commit()


def is_request_processed(request_id):
    status, _ = get_document_status(request_id)
    if status == 1.0:
        return True
    else:
        return False


def get_page_and_page_state(request_id, name):
    page = db_session.query(Page).filter(Page.request_id == request_id)\
                                 .filter(Page.name == name)\
                                 .first()
    if page:
        return page, page.state
    else:
        return None, None


def get_engine(engine_id):
    engine = db_session.query(Engine).filter(Engine.id == engine_id).first()
    return engine


def get_latest_models(engine_id):
    engine_version = db_session.query(EngineVersion).filter(EngineVersion.engine_id == engine_id).order_by(EngineVersion.id.desc()).first()
    models = db_session.query(Model)\
                       .outerjoin(EngineVersionModel)\
                       .filter(EngineVersionModel.engine_version_id == engine_version.id)\
                       .all()
    return engine_version, models


def get_document_pages(request_id):
    pages = db_session.query(Page).filter(Page.request_id == request_id).all()
    return pages


def change_page_path(request_id, page_name, new_url):
    page = db_session.query(Page).filter(Page.request_id == request_id).filter(Page.name == page_name).first()
    page.url = new_url
    page.state = PageState.WAITING
    page.waiting_timestamp = datetime.datetime.utcnow()
    db_session.commit()


def get_request_by_page(page):
    request = db_session.query(Request).filter(Request.id == page.request_id).first()
    return request


def get_api_key_by_id(api_id):
    request = db_session.query(ApiKey).filter(ApiKey.id == api_id).first()
    return request


def get_notification():
    notification = db_session.query(Notification).first()
    return notification.last_notification


def set_notification():
    notification = db_session.query(Notification).first()
    notification.last_notification = datetime.datetime.now()
    db_session.commit()
