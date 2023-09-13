#!/usr/bin/env python3

from worker_adapter.worker_adapter import WorkerAdapter, WorkerAdapterError
from worker_adapter.worker_adapter import WorkerAdapterRecoverableError
from worker_adapter.zk_adapter_client import ZkAdapterClient
from worker_adapter.mail_client import MailClient
from worker_adapter.application_adapter import ApplicationAdapter
from message_definitions.message_pb2 import ProcessingRequest
from pero_ocr.core.layout import PageLayout, create_ocr_processing_element
import worker_adapter.adapter_aux_functions as af

from app.db import model as db_model
import sqlalchemy
import numpy as np

import logging
import os
import sys
import requests
import zipfile
import magic
import argparse
import datetime
import time
import configparser


class DBClient:
    def __init__(self,
        database_url,
        max_request_count,
        max_processing_time,
        logger
    ):
        """
        :param database_url: url for database connection
        :param max_request_count: maximum number of request to process
            simultaneously
        :param max_processing_time: maximum time (in seconds) request can spend
            in processing until marked to try again
        :param logger: logger instance
        """
        self.db_engine = None
        self.new_session = None
        self.database_url = database_url
        self.max_request_count = max_request_count
        self.max_processing_time = max_processing_time
        self.logger = logger

        # current session
        self.db_session = None
    
    def db_connect(self):
        """
        Initializes db engine and sessionmaker
        """
        self.db_engine = sqlalchemy.create_engine(self.database_url)
        self.new_session = sqlalchemy.orm.scoped_session(
            sqlalchemy.orm.sessionmaker(
                bind=self.db_engine,
                autocommit=False
            )
        )
    
    def db_create_session(self):
        """
        Creates new database session.
        """
        # new_session is scoped session instance
        self.db_session = self.new_session()
    
    def rollback(self):
        """
        Rollback all pending / uncommited operations on current session.
        """
        self.db_session.rollback()
    
    def db_get_document_status(self, request_id):
        """
        Get status of document.
        :param request_id: id of processing request
        :return: tuple (status, quality)
                 status - percentage how much is processed,
                 quality - quality of the transcription
        """
        if not self.db_session:
            self.db_create_session()
        
        all = self.db_session.query(db_model.Page) \
                .filter(db_model.Page.request_id == request_id) \
                .count()
        not_processed = self.db_session.query(db_model.Page) \
                        .filter(db_model.Page.request_id == request_id) \
                        .filter(db_model.Page.state.in_([
                            db_model.PageState.CREATED,
                            db_model.PageState.WAITING,
                            db_model.PageState.PROCESSING])) \
                        .count()
        status = (all - not_processed) / all

        quality = self.db_session.query(sqlalchemy.func.avg(
                                            db_model.Page.score)) \
                    .filter(db_model.Page.request_id == request_id) \
                    .filter(db_model.Page.state == db_model.PageState\
                                                           .PROCESSED) \
                    .first()[0]

        return status, quality

    def db_is_request_processed(self, request_id):
        """
        Find out if request is processed.
        :param request_id: id of request
        :return: True if request is fully processed, False otherwise
        """
        status, _ = self.db_get_document_status(request_id)
        if status == 1.0:
            return True
        else:
            return False
    
    def db_change_page_to_not_found(self, page_id, traceback, engine_version):
        """
        Change page state to page image was not found.
        :param page_id: id of the page
        :param traceback: error traceback
        :param engine_version: version of the engine request was processed by
        """
        self.db_change_page_to_failed(
            page_id=page_id,
            fail_type=db_model.PageState.NOT_FOUND,
            traceback=traceback,
            engine_version=engine_version
        )

    def db_change_page_to_processing_failed(self,
        page_id,
        traceback,
        engine_version
    ):
        """
        Change page state to processing failed.
        :param page_id: id of the page
        :param traceback: error traceback
        :param engine_version: version of the engine request was processed by
        """
        self.db_change_page_to_failed(
            page_id=page_id,
            fail_type=db_model.PageState.PROCESSING_FAILED,
            traceback=traceback,
            engine_version=engine_version
        )

    def db_change_page_to_failed(self,
        page_id,
        fail_type,
        traceback,
        engine_version
    ):
        """
        Change page state to failed.
        :param page_id: id of the page
        :param fail_type: type of failure (datatype PageState)
        :param traceback: error traceback
        :param engine_version: version of the engine request was processed by
        """
        if not self.db_session:
            self.db_create_session()
        
        page = self.db_session.query(db_model.Page)\
                   .filter(db_model.Page.id == page_id)\
                   .first()
        request = self.db_session.query(db_model.Request)\
                      .filter(db_model.Request.id == page.request_id)\
                      .first()

        page.state = fail_type
        page.traceback = traceback
        page.engine_version = engine_version

        timestamp = datetime.datetime.utcnow()
        page.finish_timestamp = timestamp
        request.modification_timestamp = timestamp

        self.db_session.commit()
        if self.db_is_request_processed(request.id):
            request.finish_timestamp = timestamp
            self.db_session.commit()
    
    def db_change_page_to_processed(self, page_id, score, engine_version):
        """
        Change page state to processed.
        :param page_id: id of the page
        :param score: transcription quality score
        :param engine_version: version of the engine page was processed by
        """
        if not self.db_session:
            self.db_create_session()
        
        page = self.db_session.query(db_model.Page)\
                   .filter(db_model.Page.id == page_id)\
                   .first()
        request = self.db_session.query(db_model.Request)\
                      .filter(db_model.Request.id == page.request_id)\
                      .first()

        page.score = score
        page.state = db_model.PageState.PROCESSED
        page.engine_version = engine_version

        timestamp = datetime.datetime.utcnow()
        page.finish_timestamp = timestamp
        request.modification_timestamp = timestamp
        self.db_session.commit()
        if self.db_is_request_processed(request.id):
            request.finish_timestamp = timestamp
            self.db_session.commit()
    
    def get_max_upload_request_count(self):
        """
        Returns number of pages that can be uploaded to MQ.
        :raise: sqlalchemy.exc.OperationalError if DB operation fails
        """
        if not self.db_session:
            self.db_create_session()
        
        number_of_pages_in_processing = 0

        pages_in_processing = self.db_session.query(db_model.Page) \
            .join(db_model.Request) \
            .join(db_model.ApiKey) \
            .filter(db_model.ApiKey.suspension == False) \
            .filter(db_model.Page.state == db_model.PageState.PROCESSING) \
            .all()
        number_of_pages_in_processing = len(pages_in_processing)

        # Marks pages that were not processed in time to be uploaded to MQ again
        current_time = datetime.datetime.now(datetime.timezone.utc)\
                                        .replace(tzinfo=None)

        timeout_pages = 0
        for page in pages_in_processing:
            if current_time - page.processing_timestamp \
                    > datetime.timedelta(seconds=self.max_processing_time):
                page.state = db_model.PageState.WAITING
                timeout_pages += 1
                self.logger.debug(f'Marking page {page.id} for reupload.')
        
        if timeout_pages:
            self.logger.debug('Applying chages.')
            self.db_session.commit()
            number_of_pages_in_processing -= timeout_pages

        return self.max_request_count - number_of_pages_in_processing
    
    def get_waiting_pages(self):
        """
        Returns list of waiting pages from db.
        :return: list of db_model.Page in state waiting
        """
        if not self.db_session:
            self.db_create_session()
        
        return self.db_session \
                .query(db_model.Page)\
                .join(db_model.Request)\
                .join(db_model.ApiKey) \
                .filter(db_model.ApiKey.suspension == False) \
                .filter(db_model.Page.state == db_model.PageState.WAITING) \
                .all()
    
    def get_pages_to_upload(self):
        """
        Returns list of waiting pages from db, number of waiting pages does not
        exceeds maximum upload request count.
        :return: list of db_model.Page in state waiting
        """
        waiting_pages = self.get_waiting_pages()
        if len(waiting_pages) == 0:
            return []
        
        count = self.get_max_upload_request_count()
        if count == 0:
            return []
        
        return [ x[1] for x in zip(range(count), waiting_pages) ]
    
    def get_request_engine(self, request_id):
        """
        Returns engine for given reqest id.
        :return: instance of db_model.Engine
        """
        if not self.db_session:
            self.db_create_session()
        
        return self.db_session.query(db_model.Engine) \
                              .join(db_model.Request) \
                              .filter(db_model.Request.id == request_id) \
                              .first()
    
    def set_page_in_processing(self, page_id, timestamp):
        """
        Sets page status to processing.
        """
        if not self.db_session:
            self.db_create_session()
        
        page = self.db_session.query(db_model.Page) \
                              .filter(db_model.Page.id == page_id) \
                              .first()
        page.state = db_model.PageState.PROCESSING
        page.processing_timestamp = timestamp
        self.db_session.commit()


class ApiWorkerAdapter(ApplicationAdapter):
    """
    PERO OCR API Worker Adapter.
    """
    def __init__(self,
        database_url,
        max_request_count,
        max_processing_time,
        db_fetch_interval,
        upload_images_folder,
        processed_requests_folder,
        mail_subject_prefix,
        mail_client,
        logger
    ):
        """
        Init.
        :param database_url: url for database connection
        :param max_request_count: maximum number of request to process
            simultaneously
        :param max_processing_time: maximum time (in seconds) request can
            spend in processing until marked to be send to processing again
        :param upload_images_folder: folder where images waiting for upload
            are stored
        :param processed_requests_folder: folder where to write results
            of processing
        :param mail_subject_prefix: prefix added to mail subject when error
            is reported via email.
        :param mail_client: mail client providing send_mail_notification method
        :param logger: logger instance
        """
        self.db_client = DBClient(
            database_url=database_url,
            max_request_count=max_request_count,
            max_processing_time=max_processing_time,
            logger=logger
        )

        self.db_fetch_interval = db_fetch_interval

        self.upload_images_folder = upload_images_folder
        self.processed_requests_folder = processed_requests_folder

        self.mail_subject_prefix = mail_subject_prefix
        self.mail_client = mail_client

        self.waiting_pages = []

        self.logger = logger

        # connect db client to database
        self.db_client.db_connect()
    
    def get_score(self, page_layout):
        """
        Returns transcription confidence 'score'.
        Score is calculated as median from score of each line.
        :param page_layout: page layout object of given page
        :returns: page score
        """
        line_quantiles = []
        for line in page_layout.lines_iterator():
            if line.transcription_confidence is not None:
                line_quantiles.append(line.transcription_confidence)
        if not line_quantiles:
            return 1.0
        else:
            return np.quantile(line_quantiles, .50)
    
    def on_message_receive(self, processing_request: ProcessingRequest) -> None:
        """
        Saves received processing results and changes page status in database.
        :param processing_request: processing request with results to save
        """
        self.logger.debug(
            f'Receiving processing request: {processing_request.uuid}'
        )
        self.logger.debug(
            f'Request page: {processing_request.page_uuid}'
        )
        self.logger.debug(
            f'Request stage: {processing_request.processing_stages[0]}'
        )

        output_folder = os.path.join(
            self.processed_requests_folder,
            processing_request.uuid
        )
        lock_path = os.path.join(
            output_folder,
            processing_request.uuid + '_lock'
        )
        zip_path = os.path.join(
            output_folder,
            processing_request.page_uuid + '.zip'
        )
        logits_path = os.path.join(
            output_folder,
            processing_request.page_uuid + '.logits.zip'
        )

        # get status and engine version
        processed = True
        engine_version = []
        for log in processing_request.logs:
            engine_version.append(f'{log.stage}: {log.version}')
            if log.status != 'OK':
                processed == False
        engine_version = ', '.join(engine_version)

        if not processed:  # processing Failed
            try:
                self.db_client.db_change_page_to_processing_failed(
                    page_id=processing_request.page_uuid,
                    traceback=processing_request.logs[-1].log,  # save log
                    engine_version=engine_version
                )
            except:
                raise WorkerAdapterRecoverableError(
                    'Failed to save processing result of page'
                    f' {processing_request.page_uuid} to database!'
                )
            else:
                self.mail_client.send_mail_notification(
                    subject=f'{self.mail_subject_prefix} - PROCESSING FAILED',
                    body=f'{processing_request.logs[-1].log}'
                )
                return
        
        # page processed successfully
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
        
        page_img = None
        page_xml = None
        page_logits = None

        # get file references
        for i, result in enumerate(processing_request.results):
            ext = os.path.splitext(result.name)[1]
            datatype = magic.from_buffer(result.content, mime=True)
            if datatype.split('/')[0] == 'image':  # recognize image
                page_img = processing_request.results[i]
            elif ext =='.xml':
                page_xml = processing_request.results[i]
            elif ext == '.logits':
                page_logits = processing_request.results[i]

        error_msg = 'API ERROR:'
        loading_error = False

        # load pages
        page_layout = PageLayout()
        try:
            page_layout.from_pagexml_string(page_xml.content)
        except AttributeError:
            error_msg = f'{error_msg}\nFailed to load pagexml data!'
            loading_error = True
        
        try:
            page_layout.load_logits(page_logits.content)
        except AttributeError:
            error_msg = f'{error_msg}\nFailed to load logits data!'
            loading_error = True

        if loading_error:            
            # processing failed - missing output files
            try:
                self.db_client.db_change_page_to_processing_failed(
                    page_id=processing_request.page_uuid,
                    traceback=f'{processing_request.logs[-1].log}\n{error_msg}',
                    engine_version=engine_version
                )
            except:
                raise WorkerAdapterRecoverableError(
                    'Failed to save processing result of page'
                    f' {processing_request.page_uuid} to database!'
                )

            self.mail_client.send_mail_notification(
                subject=f'{self.mail_subject_prefix} - PROCESSING FAILED',
                body=f'{processing_request.logs[-1].log}\n{error_msg}'
            )
            return
        
        # generate pagexml with version
        page_xml_final = page_layout.to_pagexml_string(
            creator=f'PERO OCR: {engine_version}'
        )

        # generate altoxml format
        alto_xml = page_layout.to_altoxml_string(
            ocr_processing=create_ocr_processing_element(
                software_version_str=engine_version
            ),
            page_uuid=processing_request.page_uuid
        )

        # calculate score - from xml
        score = self.get_score(page_layout)

        # save results
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr('{}_page.xml'.format(page_img.name), page_xml_final)
            zipf.writestr(
                '{}_page.logits'.format(page_img.name),
                page_logits.content
            )
            zipf.writestr('{}_alto.xml'.format(page_img.name), alto_xml)
        
        # change state to processed in database, save score, save engine version
        # used to process the page
        try:
            self.db_client.db_change_page_to_processed(
                page_id=processing_request.page_uuid,
                score=score,
                engine_version=engine_version
            )
        except:
            raise WorkerAdapterRecoverableError(
                'Failed to save processing result of page'
                f' {processing_request.page_uuid} to database!'
            )

        # remove image from upload images folder
        if page_img.name:
            image_path = os.path.join(
                self.upload_images_folder,
                processing_request.page_uuid,
                '{}'.format(page_img.name)
            )
            if os.path.exists(image_path):
                os.unlink(image_path)

    def get_request(self) -> ProcessingRequest:
        """
        Method called by worker adapter when new processing request can be
        uploaded to MQ for processing.
        :return: processing request instance
        :raise WorkerAdapterRecoverableError when fails but adapter should
            try to call the method later again
        """
        try:
            page = None
            files = []
            engine = None

            # fetch data from DB
            if not self.waiting_pages:
                self.waiting_pages = self.db_client.get_pages_to_upload()
                self.logger.debug(
                    f'Number of pages to process: {len(self.waiting_pages)}'
                )
                # wait until some pages are available
                while not self.waiting_pages:
                    self.logger.debug(
                        f'Waiting for {self.db_fetch_interval} seconds '
                        'before fetching more pages!'
                    )
                    time.sleep(self.db_fetch_interval)
                    self.waiting_pages = self.db_client.get_pages_to_upload()
                    self.logger.debug(
                        f'Number of pages to process: {len(self.waiting_pages)}'
                    )
            
            page = self.waiting_pages[0]
            
            image_path = os.path.join(
                self.upload_images_folder,
                str(page.request_id),
                page.name
            )

            # download image
            if not os.path.exists(image_path):
                response = requests.get(url=page.url, verify=False, stream=True)
                if response.status == 200:
                    with open(image_path, 'wb') as image:
                        # load image using 2048b chunks
                        for chunk in response.iter_content(chunk_size=2048):
                            image.write(chunk)
                else:  # download failed
                    self.logger.error(
                        f'Failed to upload page {page.id} to MQ,'
                        ' file download failed!'
                    )
                    self.logger.debug(
                        f'Received error: {response.error_msg}'
                    )
                    self.db_client.db_change_page_to_not_found(
                        page_id=page.id,
                        traceback=f'Failed to download page {page.id}!'
                                f'\n{response.error_msg}',
                        engine_version=None
                    )
                    self.mail_client.send_mail_notification(
                        subject=f'{self.mail_subject_prefix}'
                                ' - Failed to upload request, page not found!',
                        body=f'Failed to download page {page.id}!'
                            f'\n{response.error_msg}',
                    )
                    self.waiting_pages.pop(0)
                    return
            
            try:
                with open(image_path, 'rb') as image:
                    files.append((page.name, image.read()))
            except OSError as e:
                self.logger.error(
                    f'Failed to upload page {page.id} to MQ,'
                    'file not accessible!'
                )
                self.logger.debug(f'Received error: {e}')
                self.db_client.db_change_page_to_not_found(
                    page_id=page.id,
                    traceback=f'Page file not accessible!\n{e}',
                    engine_version=None
                )
                self.mail_client.send_mail_notification(
                    subject=f'{self.mail_subject_prefix}'
                            ' - Page file not accessible!',
                    body=f'Failed to upload page {page.id} to processing, '
                        f'file is not accessible!\n{e}'
                )
                self.waiting_pages.pop(0)
                return
            
            engine = self.db_client.get_request_engine(page.request_id)

            return af.create_processing_request(
                request_id=str(page.request_id),
                page_id=str(page.id),
                processing_stages=engine.pipeline.split(','),
                files=files
            )
        except sqlalchemy.exc.OperationalError as e:
            self.logger.error('Database connection failed!')
            self.logger.debug(f'Received error: {e}')
            raise WorkerAdapterRecoverableError('Database connection failed!')
        except sqlalchemy.exc.PendingRollbackError as e:
            # transaction initialized before connection failure must be rolled
            # back
            self.logger.error("Rolling back invalid database transactions!")
            self.logger.debug('Received error: {}'.format(e))
            try:
                self.db_client.rollback()
            except sqlalchemy.exc.OperationalError:
                pass
            raise WorkerAdapterRecoverableError('Database connection failed!')

    def confirm_send(self,
        processing_request: ProcessingRequest,
        timestamp: datetime.datetime
    ) -> None:
        """
        Method called by worker adapter when processing request is succesfully
        send to MQ for processing. Method marks page in database as
        'in processing' and stores timestamp when processing began.
        :param processing_request: processing request that has been sent
        :param timestamp: timestamp when the request was sent
        :nothrow
        """
        try:
            self.db_client.set_page_in_processing(
                page_id=processing_request.page_uuid,
                timestamp=timestamp
            )
        except Exception:
            self.logger.error(
                f'Failed to set page {processing_reqest.page_uuid} state '
                'to \'PROCESSING\'!'
            )
            self.logger.error(f'Received error:\n{traceback.format_exc()}')
        else:
            # remove page from waiting pages
            for page in self.waiting_pages:
                if str(page.id) == processing_request.page_uuid:
                    self.waiting_pages.remove(page)

    def report_error(self,
        processing_request: ProcessingRequest,
        traceback: str
    ) -> None:
        """
        Method called when request cannot be send to processing because pipeline
        is wrongly defined and the input queue does not exist. Method marks
        page processing as failed and stores error message to database.
        :param processing_request: processing request that could not be send
        :param traceback: error traceback / error message to store to database
        :nothrow
        """
        try:
            self.db_client.db_change_page_to_processing_failed(
                page_id=processing_request.page_uuid,
                traceback=traceback,
                engine_version=None
            )
        except Exception:
            self.logger.error(
                f'Failed to set page {processing_request.page_uuid} state'
                'to \'PROCESSING_FAILED\'!'
            )
            self.logger.error(f'Received error:\n{traceback.format_exc()}')
        else:
            # remove page from waiting pages
            for page in self.waiting_pages:
                if str(page.id) == processing_request.page_uuid:
                    self.waiting_pages.remove(page)

def get_args():
    """
    Aux function for parsing commandline arguments passed to the script.
    :return: Namespace object with parsed arguments.
    """
    parser = argparse.ArgumentParser('Worker adapter for sending and receiving'
                                     ' processing requests.')
    parser.add_argument(
        '-p' ,'--publisher',
        help='Determines if adapter should run as publisher or receiver.'
             ' (default=receiver)',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '-c', '--config',
        help='Path to adapter config file.'
             ' (default=./worker_adapter_config.ini)',
        default='./worker_adapter_config.ini'
    )
    parser.add_argument(
        '-d', '--debug',
        help='Enable debug log output.',
        default=False,
        action='store_true'
    )
    return parser.parse_args()

def main():
    args = get_args()

    # Default logger settings (reuirede by kazoo library)
    log_formatter = logging.Formatter('%(asctime)s WORKER_ADAPTER '
                                      '%(levelname)s %(message)s')

    # use UTC time in log
    log_formatter.converter = time.gmtime

    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(log_formatter)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)
    logger.addHandler(stderr_handler)
    logger.propagate = False

    app_config = configparser.ConfigParser()
    app_config.read(args.config)

    mail_subject_prefix = 'API Worker Adapter'
    username = app_config['Adapter']['USERNAME']
    password = app_config['Adapter']['PASSWORD']
    ca_cert = app_config['Adapter']['CA_CERT']
    queue_name = app_config['Adapter']['QUEUE']

    mail_client = MailClient(
        user=app_config['Mail']['USERNAME'],
        password=app_config['Mail']['PASSWORD'],
        receivers=[
            a.strip() for a
                      in app_config['Mail']['NOTIFICATION_ADDRESSES'].split(',')
            ],
        host=app_config['Mail']['SERVER'],
        mail_interval=int(app_config['Mail']['MAX_EMAIL_INTERVAL']),
        logger=logger
    )
    zk_adapter_client = ZkAdapterClient(
        zookeeper_servers=app_config['Adapter']['ZOOKEEPER_SERVERS'],
        username=username,
        password=password,
        ca_cert=ca_cert,
        logger=logger
    )
    zk_adapter_client.run()
    api_worker_adapter = ApiWorkerAdapter(
        database_url=app_config['DB']['DATABASE_URL'],
        max_request_count=int(app_config['Adapter']['MAX_REQUEST_COUNT']),
        max_processing_time=int(app_config['Adapter']['MAX_PROCESSING_TIME']),
        db_fetch_interval=int(app_config['Adapter']['DB_FETCH_INTERVAL']),
        upload_images_folder=app_config['API']['UPLOAD_IMAGES_FOLDER'],
        processed_requests_folder=
            app_config['API']['PROCESSED_REQUESTS_FOLDER'],
        mail_subject_prefix=mail_subject_prefix,
        mail_client=mail_client,
        logger=logger
    )
    worker_adapter = WorkerAdapter(
        zk_client=zk_adapter_client,
        mail_client=mail_client,
        mail_subject_prefix=mail_subject_prefix,
        username=username,
        password=password,
        ca_cert=ca_cert,
        recovery_timeout=
            int(app_config['Adapter']['CONNECTION_RETRY_INTERVAL']),
        max_error_count=10,
        logger=logger
    )

    if args.publisher:
        return worker_adapter.start_uploading_requests(
            output_queue_name = queue_name,
            get_request = api_worker_adapter.get_request,
            confirm_send = api_worker_adapter.confirm_send,
            report_error = api_worker_adapter.report_error
        )
    else:
        return worker_adapter.start_receiving_results(
            queue_name = queue_name,
            on_message_receive = api_worker_adapter.on_message_receive
        )

if __name__ == "__main__":
    sys.exit(main())
