import os
import os.path
import zipfile
import datetime
import dateutil.parser
import traceback
from io import BytesIO
from urllib.parse import urlparse
from filelock import FileLock, Timeout
from flask import redirect, request, jsonify, send_file, abort
from pathlib import Path
from app.main import bp
from app.db.api_key import require_user_api_key, require_super_user_api_key
from app.db.model import PageState
from flask import current_app as app
from flask import render_template
from app.main.general import create_request, request_exists, cancel_request_by_id, \
                             get_engine_dict, get_page_by_id, check_save_path, get_page_by_preferred_engine, \
                             request_belongs_to_api_key, get_engine_by_page_id, \
                             change_page_to_processed, get_page_and_page_state, get_engine, \
                             get_document_pages, change_page_to_failed, get_page_statistics, change_page_path, \
                             get_request_by_page, get_notification, set_notification, get_api_key_by_id, \
                             get_usage_statistics
from app.mail.mail import send_mail


@bp.route('/')
@bp.route('/index')
def index():
    state_stats, _ = get_page_statistics()
    return render_template('index.html', data=state_stats)


@bp.route('/docs')
def documentation():
    return redirect('https://app.swaggerhub.com/apis-docs/LachubCz/PERO-API/1.0.4')


@bp.route('/post_processing_request', methods=['POST'])
@require_user_api_key
def post_processing_request():
    api_string = request.headers.get('api-key')
    try:
        db_request, engine_id = create_request(api_string, request.json)
    except:
        exception = traceback.format_exc()
        return jsonify({
            'status': 'failure',
            'message': 'Bad JSON formatting.',
            'error_message': exception.encode('utf-8')}), 422
    else:
        if db_request is not None:
            return jsonify({
                'status': 'success',
                'request_id': db_request.id}), 200
        else:
            return jsonify({
                'status': 'failure',
                'message': f'Engine {engine_id} has not been found.'}), 404


@bp.route('/usage_statistics', methods=['GET'])
@bp.route('/usage_statistics/<string:from_datetime>', methods=['GET'])
@bp.route('/usage_statistics/<string:from_datetime>/<string:to_datetime>', methods=['GET'])
@require_user_api_key
def usage_statistics(from_datetime=None, to_datetime=None):
    if from_datetime:
        try:
            from_datetime = dateutil.parser.isoparse(from_datetime)
        except ValueError:
            return 'from_time is not in a valid ISO format.', 400

    if to_datetime:
        try:
            to_datetime = dateutil.parser.isoparse(to_datetime)
        except ValueError:
            return 'to_datetime is not in a valid ISO format.', 400

    api_string = request.headers.get('api-key')
    count = get_usage_statistics(api_string, from_datetime=from_datetime, to_datetime=to_datetime)
    result = {'status': 'success', 'processed_pages': count}
    if from_datetime:
        result['from'] = from_datetime.isoformat()
    if to_datetime:
        result['to'] = to_datetime.isoformat()

    return jsonify(result), 200


@bp.route('/upload_image/<string:request_id>/<string:page_name>', methods=['POST'])
@require_user_api_key
def upload_image(request_id, page_name):
    request_ = request_exists(request_id)
    api_string = request.headers.get('api-key')
    if not request_:
        return jsonify({
            'status': 'failure',
            'message': f'Request {request_id} does not exist.'}), 404
    if not request_belongs_to_api_key(request.headers.get('api-key'), request_id):
        return jsonify({
            'status': 'failure',
            'message': f'Request {request_id} does not belong to API key {api_string}.'}), 401
    page, page_state = get_page_and_page_state(request_id, page_name)
    if not page:
        return jsonify({
            'status': 'failure',
            'message': f'Page {page_name} does not exist.'}), 404
    if page_state != PageState.CREATED:
        return jsonify({
            'status': 'failure',
            'message': f'Page {page_name} is in {page_state.name} state. It should be in CREATED state.'}), 400

    if 'file' not in request.files:
        return jsonify({
            'status': 'failure',
            'message': 'Request does not contain file.'}), 400

    file = request.files['file']
    extension = os.path.splitext(file.filename)[1][1:].lower()
    if file and extension in app.config['ALLOWED_IMAGE_EXTENSIONS']:
        Path(os.path.join(app.config['UPLOAD_IMAGES_FOLDER'], str(page.request_id))).mkdir(parents=True, exist_ok=True)
        file.save(os.path.join(app.config['UPLOAD_IMAGES_FOLDER'], str(page.request_id), page_name))
        o = urlparse(request.base_url)
        path = f'{o.scheme}://{o.netloc}{app.config["APPLICATION_ROOT"]}/download_image/{request_id}/{page_name}'
        change_page_path(request_id, page_name, path)
        return jsonify({
            'status': 'success'})
    else:
        allowed_extesions = str(app.config["ALLOWED_IMAGE_EXTENSIONS"]).replace("\'", "")[1:-1]
        return jsonify({
            'status': 'failure',
            'message': f'{extension} is not supported format. Supported formats are {allowed_extesions}.'}), 422


@bp.route('/request_status/<string:request_id>', methods=['GET'])
@require_user_api_key
def request_status(request_id):
    api_string = request.headers.get('api-key')
    if not request_exists(request_id):
        return jsonify({
            'status': 'failure',
            'message': f'Request {request_id} does not exist.'}), 404

    if not request_belongs_to_api_key(request.headers.get('api-key'), request_id):
        return jsonify({
            'status': 'failure',
            'message': f'Request {request_id} does not belong to API key {api_string}.'}), 401

    pages = get_document_pages(request_id)

    return jsonify({
        'status': 'success',
        'request_status': {page.name: {'state': str(page.state).split('.')[1], 'quality': page.score} for page in pages}}), 200


@bp.route('/get_engines', methods=['GET'])
@require_user_api_key
def get_engines():
    engines = get_engine_dict()
    return jsonify({
        'status': 'success',
        'engines': engines}
    )


@bp.route('/download_results/<string:request_id>/<string:page_name>/<string:format>', methods=['GET'])
@require_user_api_key
def download_results(request_id, page_name, format):
    api_string = request.headers.get('api-key')
    request_ = request_exists(request_id)
    if not request_:
        return jsonify({
            'status': 'failure',
            'message': f'Request {request_id} does not exist.'}), 404
    if not request_belongs_to_api_key(request.headers.get('api-key'), request_id):
        return jsonify({
            'status': 'failure',
            'message': f'Request {request_id} does not belong to API key {api_string}.'}), 401
    page, page_state = get_page_and_page_state(request_id, page_name)
    if not page:
        return jsonify({
            'status': 'failure',
            'message': f'Page {page_name} does not exist.'}), 404
    if page_state == PageState.EXPIRED:
        return jsonify({
            'status': 'failure',
            'message': f'Page {page_name} has expired. All processed pages are stored one week.'}), 404
    if page_state != PageState.PROCESSED:
        return jsonify({
            'status': 'failure',
            'message': f'Page {page_name} is not processed yet.'}), 404
    if format not in ['alto', 'page', 'txt']:
        return jsonify({
            'status': 'failure',
            'message': 'Bad export format. Supported formats are alto, page, txt.'}), 400

    try:
        with FileLock(os.path.join(app.config['PROCESSED_REQUESTS_FOLDER'], str(page.request_id), str(page.request_id)+'_lock'), timeout=1):
            archive = zipfile.ZipFile(os.path.join(app.config['PROCESSED_REQUESTS_FOLDER'], str(request_.id), str(page.id)+'.zip'), 'r')
            if format == 'alto':
                data = archive.read('{}_alto.xml'.format(page.name))
                extension = 'xml'
            elif format == 'page':
                data = archive.read('{}_page.xml'.format(page.name))
                extension = 'xml'
            elif format == 'txt':
                data = archive.read('{}.txt'.format(page.name))
                extension = 'txt'
    except Timeout:
        archive = zipfile.ZipFile(os.path.join(app.config['PROCESSED_REQUESTS_FOLDER'], str(request_.id), str(page.id) + '.zip'), 'r')
        if format == 'alto':
            data = archive.read('{}_alto.xml'.format(page.name))
            extension = 'xml'
        elif format == 'page':
            data = archive.read('{}_page.xml'.format(page.name))
            extension = 'xml'
        elif format == 'txt':
            data = archive.read('{}.txt'.format(page.name))
            extension = 'txt'

    return send_file(BytesIO(data),
                     attachment_filename='{}.{}'.format(page.name, extension),
                     as_attachment=True)


@bp.route('/cancel_request/<string:request_id>', methods=['POST'])
@require_user_api_key
def cancel_request(request_id):
    api_string = request.headers.get('api-key')
    if not request_exists(request_id):
        return jsonify({
            'status': 'failure',
            'message': f'Request {request_id} does not exist.'}), 404

    if not request_belongs_to_api_key(request.headers.get('api-key'), request_id):
        return jsonify({
            'status': 'failure',
            'message': f'Request {request_id} does not belong to API key {api_string}.'}), 401

    cancel_request_by_id(request_id)
    return jsonify({
        'status': 'success'}), 200


@bp.route('/page_statistics', methods=['GET'])
@require_super_user_api_key
def page_statistics():
    state_stats, engine_stats = get_page_statistics()

    return jsonify({
        'status': 'success',
        'state_stats': state_stats,
        'engine_stats': engine_stats}), 200


@bp.errorhandler(500)
def handle_exception(e):
    if app.config['EMAIL_NOTIFICATION_ADDRESSES'] != []:
        send_mail(subject="API Bot - INTERNAL SERVER ERROR",
                  body=traceback.format_exc().replace("\n", "<br>"),
                  sender=('PERO OCR - API BOT', app.config['MAIL_USERNAME']),
                  recipients=app.config['EMAIL_NOTIFICATION_ADDRESSES'],
                  host=app.config['MAIL_SERVER'],
                  password=app.config['MAIL_PASSWORD'])

    abort(500)
