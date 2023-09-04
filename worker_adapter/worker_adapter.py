#!/usr/bin/env python3

# Worker adapter - connects api to workers via Apache Zookeeper and RabbitMQ
# Sends processing requests to worker system and receives processed results

import worker_functions.connection_aux_functions as cf
import worker_functions.constants as constants
from worker_functions.zk_client import ZkClient
from worker_functions.mq_client import MQClient
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data

from pero_ocr.core.layout import PageLayout, create_ocr_processing_element

from app.mail.mail import send_mail
from app.db import model as db_model
from google.protobuf.timestamp_pb2 import Timestamp
import sqlalchemy
import numpy as np

import kazoo
import pika
import logging

import os
import sys
import threading
import copy
import zipfile
from filelock import FileLock, Timeout
import magic
import time
import requests
import datetime
import configparser
import argparse
import traceback

class DBClient:
    def __init__(self, database_url, max_request_count, max_processing_time, logger):
        """
        :param database_url: url for database connection
        :param max_request_count: maximum number of request to process simultaneously
        :param max_processing_time: maximum time (in seconds) request can spend in processing
                                    until marked to try again
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
        self.new_session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker(bind=self.db_engine, autocommit=False))
    
    def db_create_session(self):
        """
        Creates new database session.
        """
        self.db_session = self.new_session()  # new_session - scoped session instance
    
    def rollback(self):
        """
        Rollback all pending / uncommited operations on current session.
        """
        self.db_session.rollback()
    
    def db_get_document_status(self, request_id):
        """
        Get status of document.
        :param request_id: id of processing request
        :return: status - percentage how much is processed, quality - quality of the transcription
        """
        if not self.db_session:
            self.db_create_session()
        
        all = self.db_session.query(db_model.Page) \
                .filter(db_model.Page.request_id == request_id) \
                .count()
        not_processed = self.db_session.query(db_model.Page) \
                        .filter(db_model.Page.request_id == request_id) \
                        .filter(db_model.Page.state.in_([db_model.PageState.CREATED, db_model.PageState.WAITING, db_model.PageState.PROCESSING])) \
                        .count()
        status = (all - not_processed) / all

        quality = self.db_session.query(sqlalchemy.func.avg(db_model.Page.score)) \
                    .filter(db_model.Page.request_id == request_id) \
                    .filter(db_model.Page.state == db_model.PageState.PROCESSED) \
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

    def db_change_page_to_processing_failed(self, page_id, traceback, engine_version):
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

    def db_change_page_to_failed(self, page_id, fail_type, traceback, engine_version):
        """
        Change page state to failed.
        :param page_id: id of the page
        :param fail_type: type of failure (datatype PageState)
        :param traceback: error traceback
        :param engine_version: version of the engine request was processed by
        """
        if not self.db_session:
            self.db_create_session()
        
        page = self.db_session.query(db_model.Page).filter(db_model.Page.id == page_id).first()
        request = self.db_session.query(db_model.Request).filter(db_model.Request.id == page.request_id).first()

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
        
        page = self.db_session.query(db_model.Page).filter(db_model.Page.id == page_id).first()
        request = self.db_session.query(db_model.Request).filter(db_model.Request.id == page.request_id).first()

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

        # Marks pages that were not processed in time to be uploaded to MQ again.
        current_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        self.logger.debug(f'Current time: {current_time}')

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
                .query(db_model.Page).join(db_model.Request).join(db_model.ApiKey) \
                .filter(db_model.ApiKey.suspension == False) \
                .filter(db_model.Page.state == db_model.PageState.WAITING) \
                .all()
    
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


class ZkAdapterClient(ZkClient):
    def __init__(self, zookeeper_servers, username, password, ca_cert, logger):
        """
        Initializes zookeeper adapter client.
        :param zookeeper_servers: list of zookeeper servers to connect to
        :param username: username for zookeeper authentication
        :param password: password for zookeeper authentication
        :param ca_cert: CA certificate for zookeeper SSL verification and connection encrtyption
        :param logger: logger instance to use
        """
        super().__init__(
            zookeeper_servers=zookeeper_servers,
            username=username,
            password=password,
            ca_cert=ca_cert,
            logger=logger
        )

        # mq servers
        self.mq_servers_lock = threading.Lock()
        self.mq_servers = []
    
    def __del__(self):
        super().__del__()
    
    def zk_callback_update_mq_servers(self, servers):
        """
        Zookeeper callback for updating MQ server list.
        :param servers: list of servers
        """
        self._set_mq_servers(cf.server_list(servers))
    
    def register_update_mq_server_callback(self):
        """
        Registers zk_callback_update_mq_servers callback in zookeeper.
        """
        self.zk.ChildrenWatch(
            path=constants.WORKER_CONFIG_MQ_SERVERS,
            func=self.zk_callback_update_mq_servers
        )
    
    def _set_mq_servers(self, servers):
        """
        Sets server list.
        :param servers: new list of MQ servers
        """
        self.mq_servers_lock.acquire()
        self.mq_servers = servers
        self.mq_servers_lock.release()

    def get_mq_servers(self):
        """
        Returns current copy of MQ server list
        :return: MQ server list
        """
        self.mq_servers_lock.acquire()
        mq_servers = copy.deepcopy(self.mq_servers)
        self.mq_servers_lock.release()
        return mq_servers
    
    def run(self):
        """
        Runs the worker adapter zookeeper client.
        """
        self.zk_connect()
        self.register_update_mq_server_callback()


class WorkerAdapter(MQClient):
    def __init__(self, config, logger):
        """
        Initializes worker adapter.
        :param config: API config object instance
        :param logger: logger to use for logging
        """
        # init MQClient
        super().__init__(
            mq_servers=[],
            username=config['Adapter']['USERNAME'],
            password=config['Adapter']['PASSWORD'],
            ca_cert=config['Adapter']['CA_CERT'],
            logger=logger
        )

        # init db client
        self.db_client = DBClient(
            database_url=config['DB']['database_url'],
            max_request_count=int(config['Adapter']['MAX_REQUEST_COUNT']),
            max_processing_time=int(config['Adapter']['MAX_PROCESSING_TIME']),
            logger=logger
        )

        # init zk client
        self.zk_client = ZkAdapterClient(
            zookeeper_servers=config['Adapter']['ZOOKEEPER_SERVERS'],
            username=config['Adapter']['USERNAME'],
            password=config['Adapter']['PASSWORD'],
            ca_cert=config['Adapter']['CA_CERT'],
            logger=logger
        )

        # config
        self.config = config
        self.last_mail_time = datetime.datetime(year=1970, month=1, day=1, tzinfo=datetime.timezone.utc)
        self.mail_interval = datetime.timedelta(seconds = int(self.config['Mail']['MAX_EMAIL_INTERVAL']))
        self.notification_addresses = [ address.strip() for address in self.config['Mail']['NOTIFICATION_ADDRESSES'].split(',') ]

        # error counter
        self.error_count = 0

    def __del__(self):
        super().__del__()
    
    ### MAIL ###
    def send_mail(self, subject, body):
        """
        Sends mail with given subject and body to addresses specified in cofiguration.
        :param subject: mail subject
        :param body: mail body
        :nothrow
        """
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        if timestamp - self.last_mail_time < self.mail_interval:
            return
        
        if not self.notification_addresses:
            return

        self.last_mail_time = timestamp

        try:
            send_mail(
                subject=subject,
                body=body.replace("\n", "<br>"),
                sender=('PERO OCR - API BOT', self.config['Mail']['USERNAME']),
                password=self.config['Mail']['PASSWORD'],
                recipients=self.notification_addresses,
                host=self.config['Mail']['SERVER']
            )
        except Exception:
            self.logger.error('Failed to send notification email!')
            self.logger.debug(f'Received error:\n{traceback.format_exc()}')

    ### STATE ###
    def get_mq_servers(self):
        """
        Gets current list of MQ servers.
        :return: MQ server list
        """
        return self.zk_client.get_mq_servers()
    
    ### MQ ###
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
    
    def _mq_receive_result_callback(self, channel, method, properties, body):
        """
        Callback function for receiving processed messages from message broker
        :param channel: channel from which the message is originated
        :param method: message delivery method
        :param properties: additional message properties
        :param body: message body (actual processing request/result)
        """
        try:
            processing_request = ProcessingRequest().FromString(body)
        except Exception as e:
            self.logger.error('Failed to parse received request!')
            self.logger.error('Received error:\n{}'.format(traceback.format_exc()))
            channel.basic_nack(delivery_tag = method.delivery_tag, requeue = True)
            raise
        
        current_stage = processing_request.processing_stages[0]

        self.logger.debug(f'Receiving processing request: {processing_request.uuid}')
        self.logger.debug(f'Request page: {processing_request.page_uuid}')
        self.logger.debug(f'Request stage: {current_stage}')

        output_folder = os.path.join(self.config['API']['PROCESSED_REQUESTS_FOLDER'], processing_request.uuid)
        lock_path = os.path.join(output_folder, processing_request.uuid + '_lock')
        zip_path = os.path.join(output_folder, processing_request.page_uuid + '.zip')
        logits_path = os.path.join(output_folder, processing_request.page_uuid + '.logits.zip')
        
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
                channel.basic_nack(delivery_tag = method.delivery_tag, requeue = True)
                raise

            # send email notification
            self.send_mail(
                subject='API Bot - PROCESSING FAILED',
                body=processing_request.logs[-1].log
            )
            # acknowledge the message
            self.mq_channel.basic_ack(delivery_tag=method.delivery_tag)
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
                    traceback=f'{processing_request.logs[-1].log}\n{error_msg}',  # save log and error message
                    engine_version=engine_version
                )
            except:
                channel.basic_nack(delivery_tag = method.delivery_tag, requeue = True)
                raise

            # send email notification
            self.send_mail(
                subject='API Bot - PROCESSING FAILED',
                body=f'{processing_request.logs[-1].log}\n{error_msg}'
            )

            # acknowledge the message
            self.mq_channel.basic_ack(delivery_tag=method.delivery_tag)
            return
        
        # generate pagexml with version
        page_xml_final = page_layout.to_pagexml_string(creator=f'PERO OCR: {engine_version}')

        # generate altoxml format
        alto_xml = page_layout.to_altoxml_string(
            ocr_processing=create_ocr_processing_element(software_version_str=engine_version),
            page_uuid=processing_request.page_uuid
        )

        # calculate score - from xml
        score = self.get_score(page_layout)

        # save results
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr('{}_page.xml'.format(page_img.name), page_xml_final)
            zipf.writestr('{}_page.logits'.format(page_img.name), page_logits.content)
            zipf.writestr('{}_alto.xml'.format(page_img.name), alto_xml)
        
        # change state to processed in database, save score, save engine version
        try:
            self.db_client.db_change_page_to_processed(
                page_id=processing_request.page_uuid,
                score=score,
                engine_version=engine_version
            )
        except:
            channel.basic_nack(delivery_tag = method.delivery_tag, requeue = True)
            raise
        # remove image from config['UPLOAD_IMAGES_FOLDER']
        if page_img.name:
            image_path = os.path.join(self.config['API']['UPLOAD_IMAGES_FOLDER'], processing_request.page_uuid, '{}'.format(page_img.name))
            if os.path.exists(image_path):
                os.unlink(image_path)

        # acknowledge the message
        self.mq_channel.basic_ack(delivery_tag=method.delivery_tag)

        # reset error counter
        self.error_count = 0

    def mq_receive_results(self, queue):
        """
        Receives results from MQ server from given queue.
        :param queue: queue from where results are downloaded
        """
        while True:
            # connection init
            try:
                self.mq_connect_retry(confirm_delivery=True)
            except ConnectionError:
                self.logger.error('Failed to connect to MQ servers, trying again!')
                if self.error_count > 2:
                    self.send_mail(
                        'API Bot - MQ server connection failed!',
                        f'{e}'
                    )
                self.error_count += 1
                continue
            
            self.mq_channel.basic_consume(
                queue,
                self._mq_receive_result_callback,
                auto_ack=False
            )
            
            try:
                # receive messages
                self.mq_channel.start_consuming()
            except KeyboardInterrupt:
                # user exit
                self.logger.info('Result receiving stoped.')
                break
            except pika.exceptions.AMQPError as e:
                # connection failed - continue / recover
                self.logger.error('Result receiving failed due to MQ connection error!')
                self.logger.debug('Received error: {}'.format(e))
                if self.error_count > 2:
                    self.send_mail(
                        'API Bot - Result receiving failed due to MQ connection error!',
                        f'{e}'
                    )
                self.error_count += 1
            except sqlalchemy.exc.OperationalError as e:
                # connection to database failed
                self.logger.error('Result receiving failed due to database connection error!')
                self.logger.debug('Received error: {}'.format(e))
                if self.error_count > 2:
                    self.send_mail(
                        'API Bot - Result receiving failed due to database connection error!',
                        f'{e}'
                    )
                self.error_count += 1
                time.sleep(int(self.config['Adapter']['DB_FETCH_INTERVAL']))
            except sqlalchemy.exc.PendingRollbackError as e:
                # transaction initialized before connection failure must be rolled back
                self.logger.error("Rolling back invalid database transactions!")
                self.logger.debug('Received error: {}'.format(e))
                try:
                    self.db_client.rollback()
                except sqlalchemy.exc.OperationalError:
                    self.logger.error('Database connection failed!')
            except Exception as e:
                # unrecoverable error - exit
                self.logger.error('Result receiving failed!')
                self.logger.debug('Received error:\n{}'.format(traceback.format_exc()))
                self.send_mail(
                    'API Bot - Result receiving failed due to unknown error!',
                    f'{traceback.format_exc()}'
                )
                self.error_count += 1
                raise

    def mq_send_request(self, page, engine, output_queue):
        """
        Send request with page for processing.
        :param page: page to include in request
        :param engine: engine to use for processing
        :param output_queue: api output queue name
        :return: timestamp when request was send
        :raise: pika.exceptions.AMQPError when connection error has ocured
        :raise: pika.exceptions.UnroutableError when message is not routable (bad processing pipeline is specified)
        :raise: ConnectionError when image specified by url cannot be downloaded
        :raise: OSError when file specified by path can't be opened
        """
        image_path = os.path.join(self.config['API']['UPLOAD_IMAGES_FOLDER'], str(page.request_id), page.name)
        # download image
        if not image_path:
            response = requests.get(url=page.url, verify=False, stream=True)
            if response.status == 200:
                with open(image_path, 'wb') as image:
                    # load image using 2048b chunks
                    for chunk in response.iter_content(chunk_size=2048):
                        image.write(chunk)
            else:
                raise ConnectionError('Invalid image url!')
        
        # create processing request
        request = ProcessingRequest()
        request.uuid = str(page.request_id)  # convert from UUID object to str
        request.page_uuid = str(page.id)  # convert from UUID object to str
        request.priority = 0  # priority is deprecated and will be removed in future
        for stage in engine.pipeline.split(','):
            request.processing_stages.append(stage.strip())
        request.processing_stages.append(output_queue)  # add output queue

        with open(image_path, 'rb') as image:
            img = request.results.add()
            img.name = page.name
            img.content = image.read()
        
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        Timestamp.FromDatetime(request.start_time, timestamp)
        
        self.mq_channel.basic_publish('', engine.pipeline.split(',')[0].strip(), request.SerializeToString(),
            properties=pika.BasicProperties(delivery_mode=2, priority=request.priority),
            mandatory=True
        )

        return timestamp

    def mq_upload_requests(self, output_queue):
        """
        Periodically uploades waiting processing requests to MQ.
        :param output_queue: name of output queue where api receiver can pickup complete tasks
        """
        while True:
            try:
                try:
                    self.mq_connect_retry(confirm_delivery=True)
                except ConnectionError:
                    self.logger.error('Failed to connect to MQ servers, trying again!')
                    if self.error_count > 2:
                        self.send_mail(
                            'API Bot - MQ server connection failed!',
                            f'{e}'
                        )
                    self.error_count += 1
                    continue
                
                # get maximum number of requests that can be uploaded
                upload_request_count = self.db_client.get_max_upload_request_count()

                # fetch new pages from database
                waiting_pages = self.db_client.get_waiting_pages()

                if not waiting_pages or upload_request_count <= 0:
                    # wait for some time if no pages could be uploaded (to prevent agressive fetching from db)
                    time.sleep(int(self.config['Adapter']['DB_FETCH_INTERVAL']))
                    continue

                # upload as many requests as possible
                for page, i in zip(waiting_pages, range(upload_request_count)):
                    # Add engine (pipeline)
                    engine = self.db_client.get_request_engine(page.request_id)

                    timestamp = None

                    # upload request to MQ
                    try:
                        timestamp = self.mq_send_request(page, engine, output_queue)
                    except pika.exceptions.UnroutableError as e:
                        self.logger.error(f'Failed to upload page {page.id} due to wrong route im MQ!')
                        self.logger.debug(f'Received error: {e}')
                        self.send_mail(
                            subject='API Bot - Failed to upload request, bad route!',
                            body=f'{e}'
                        )
                        time.sleep(int(self.config['Adapter']['DB_FETCH_INTERVAL']))
                    except pika.exceptions.AMQPError as e:
                        self.logger.error('Failed to upload processing request due to MQ connection error!')
                        self.logger.debug('Received error: {}'.format(e))
                        if self.error_count > 2:
                            self.send_mail(
                                subject='API Bot - Request upload failed!',
                                body=f'{e}'
                            )
                        time.sleep(int(self.config['Adapter']['DB_FETCH_INTERVAL']))
                        self.error_count += 1
                        break
                    except OSError as e:
                        self.logger.error(f'Failed to upload page {page.id} to MQ, file not accessible!')
                        self.logger.debug(f'Received error: {e}')
                        self.send_mail(
                            subject='API Bot - Failed to upload request, file not accessible!',
                            body=f'{e}'
                        )
                        time.sleep(int(self.config['Adapter']['DB_FETCH_INTERVAL']))
                    except ConnectionError as e:
                        self.logger.error(f'Failed to upload page {page.id} to MQ, page not found!')
                        self.logger.debug(f'Received error: {e}')
                        self.db_client.db_change_page_to_not_found(
                            page_id = page.id,
                            traceback = f'{e}',
                            engine_version = None
                        )
                        self.send_mail(
                            subject='API Bot - Failed to upload request, page not found!',
                            body=f'{e}'
                        )
                    except KeyboardInterrupt:
                        raise
                    except Exception:
                        error = traceback.format_exc()
                        self.logger.error(f'Failed to upload page {page.id} to MQ!')
                        self.logger.debug(f'Received error:\n{error}')
                        self.send_mail(
                            subject='API Bot - Failed to upload request!',
                            body=f'{error}'
                        )
                        time.sleep(int(self.config['Adapter']['DB_FETCH_INTERVAL']))
                    else:
                        # update page after successfull upload
                        self.db_client.set_page_in_processing(page.id, timestamp)
                        self.error_count = 0
                        self.logger.debug(f'Page {page.id} uploaded successfully!')
                
            except KeyboardInterrupt:
                self.logger.info('Stopped request uploading!')
                break
            except sqlalchemy.exc.OperationalError as e:
                self.logger.error('Database connection failed!')
                self.logger.debug(f'Received error: {e}')
                if self.error_count > 2:
                    self.send_mail(
                        subject='API Bot - Database connection failed!',
                        body=f'{e}'
                    )
                self.error_count += 1
                time.sleep(int(self.config['Adapter']['DB_FETCH_INTERVAL']))
            except sqlalchemy.exc.PendingRollbackError as e:
                # transaction initialized before connection failure must be rolled back
                self.logger.error("Rolling back invalid database transactions!")
                self.logger.debug('Received error: {}'.format(e))
                try:
                    self.db_client.rollback()
                except sqlalchemy.exc.OperationalError:
                    self.logger.error('Database connection failed!')


    ### MAIN ###
    def run(self, publisher=False):
        """
        Starts receiving or publishing requests from/to MQ.
        :param publisher: Determines if adapter is publisher or receiver (default)
        :param queue: Output queue where adapter can pickup processing results
        :return: 0 - success / Raises exception otherwise
        """
        # start ZK client to get list of MQ server
        self.zk_client.run()

        # connect DB
        self.db_client.db_connect()

        # get queue
        queue = self.config['Adapter']['QUEUE']

        # run adapter
        if publisher:
            self.mq_upload_requests(queue)
        else:
            self.mq_receive_results(queue)
        
        return 0

def get_args():
    """
    Aux function for parsing commandline arguments passed to the script.
    :return: Namespace object with parsed arguments.
    """
    parser = argparse.ArgumentParser('Worker adapter for sending and receiving processing requests.')
    parser.add_argument(
        '-p' ,'--publisher',
        help='Determines if adapter should run as publisher or receiver. (default=receiver)',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '-c', '--config',
        help='Path to adapter config file. (default=./worker_adapter_config.ini)',
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
    log_formatter = logging.Formatter('%(asctime)s WORKER_ADAPTER %(levelname)s %(message)s')

    # use UTC time in log
    log_formatter.converter = time.gmtime

    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(log_formatter)

    logger = logging.getLogger(__name__)
    print(logger.hasHandlers())
    logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)
    logger.addHandler(stderr_handler)
    logger.propagate = False

    app_config = configparser.ConfigParser()
    app_config.read(args.config)

    worker_adapter = WorkerAdapter(
        config=app_config,
        logger=logger
    )

    worker_adapter.run(publisher=args.publisher)

    return 0
    
if __name__ == "__main__":
    sys.exit(main())