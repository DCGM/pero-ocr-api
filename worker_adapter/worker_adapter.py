#!/usr/bin/env python3

# Worker adapter - connects api to workers via Apache Zookeeper and RabbitMQ
# Sends processing requests to worker system and receives processed results

import worker_libs.worker_functions.connection_aux_functions as cf
import worker_libs.worker_functions.constants as constants
from worker_libs.worker_functions.zk_client import ZkClient
from worker_libs.worker_functions.mq_client import MQClient
from worker_libs.message_definitions.message_pb2 import ProcessingRequest, StageLog, Data

from app.db import model as db_model
from google.protobuf.timestamp_pb2 import Timestamp
import sqlalchemy

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

# Default logger settings (reuirede by kazoo library)
log_formatter = logging.Formatter('%(asctime)s WORKER_ADAPTER %(levelname)s %(message)s')

stderr_handler = logging.StreamHandler()
stderr_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# TODO - remove debug level
logger.setLevel(logging.DEBUG)
logger.addHandler(stderr_handler)

class DBClient:
    def __init__(self, database_url):
        """
        :param database_url: url for database connection
        """
        self.db_engine = None
        self.new_session = None
        self.database_url = database_url
    
    def db_connect(self):
        """
        Initializes db engine and sessionmaker
        """
        self.db_engine = sqlalchemy.create_engine(self.database_url)
        self.new_session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker(bind=self.db_engine, autocommit=False))
    
    def db_create_session(self):
        """
        Creates new database session.
        :return: Scoped database session
        """
        return self.new_session()  # new_session - scoped session instance

class WorkerAdapterMQClient(MQClient):
    """
    MQ Client for worker adapter.
    Can be used for publishing/receiving processing results.
    """

    def __init__(self, worker_adapter, config, request_count_lock, logger = logging.getLogger(__name__)):
        """
        Initializes client
        :param worker_adapter: Worker adapter object that client belongs to (calling object). Used for managing database and zookeeper session and state.
        :param config: app config object instance
        :param request_count_lock: shared semaphore object to synchronize publisher and receiver
        :param logger: Logger to user for logging messages
        """
        super().__init__(mq_servers=[], username=config['AdapterConfig']['USERNAME'], password=config['AdapterConfig']['PASSWORD'], ca_cert=config['AdapterConfig']['CA_CERT'], logger=logger)
        self.worker_adapter = worker_adapter
        self.config = config
        self.request_count_lock = request_count_lock
        self.db_session = None
        self.db_fetch_interval = int(self.config['AdapterConfig']['MIN_DB_FETCH_INTERVAL'])
    
    def __del__(self):
        """
        Destructor for MQ client.
        Uses MQClient's inherited destructor.
        """
        super().__del__()
    
    def db_get_document_status(self, request_id):
        """
        Get status of document.
        :param request_id: id of processing request
        :return: status - percentage how much is processed, quality - quality of the transcription
        """
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
    
    def db_change_page_to_failed(self, page_id, fail_type, traceback, engine_version):
        """
        Change page state to failed.
        :param page_id: id of the page
        :param fail_type: type of failure (datatype PageState)
        :param traceback: error traceback
        :param engine_version: version of the engine request was processed by
        """
        page = self.db_session.query(db_model.Page).filter(db_model.Page.id == page_id).first()
        request = self.db_session.query(db_model.Request).filter(db_model.Request.id == page.request_id).first()

        if fail_type == 'NOT_FOUND':
            page.state = db_model.PageState.NOT_FOUND
        elif fail_type == 'INVALID_FILE':
            page.state = db_model.PageState.INVALID_FILE
        elif fail_type == 'PROCESSING_FAILED':
            page.state = db_model.PageState.PROCESSING_FAILED
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
    
    def mq_connect(self, max_retry=0):
        """
        Connect to message broker servers.
        :param max_retry: maximum number of connection attempts before giving up (default - try forever)
        :raise: ConnectionError if connection to all MQ servers fails
        """
        retry_count = 0

        while not (self.mq_connection and self.mq_connection.is_open and self.mq_channel and self.mq_channel.is_open):
            if retry_count != 0:
                self.logger.warning(
                    'Failed to connect to MQ servers, waiting for {n} seconds to retry!'
                    .format(n=int(self.config['AdapterConfig']['CONNECTION_RETRY_INTERVAL']))
                )
                time.sleep(int(self.config['AdapterConfig']['CONNECTION_RETRY_INTERVAL']))

            self.mq_servers = self.worker_adapter.get_mq_servers()
            super().mq_connect()

            retry_count += 1
            if max_retry and retry_count == max_retry:
                break

        if not (self.mq_connection and self.mq_connection.is_open and self.mq_channel and self.mq_channel.is_open):
            raise ConnectionError('Failed to connect to MQ servers!')
        
        self.mq_channel.confirm_delivery()
    
    def _mq_receive_result_callback(self, channel, method, properties, body):
        """
        Callback function for receiving processed messages from message broker
        :param channel: channel from which the message is originated
        :param method: message delivery method
        :param properties: additional message properties
        :param body: message body (actual processing request/result)
        """
        # save results to db and disk
        if not self.db_session:
            self.db_session = self.worker_adapter.db_create_session()

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

        output_folder = os.path.join(self.config['APIConfig']['PROCESSED_REQUESTS_FOLDER'], processing_request.uuid)
        lock_path = os.path.join(output_folder, processing_request.uuid + '_lock')
        zip_path = os.path.join(output_folder, processing_request.page_uuid + '.zip')
        logits_path = os.path.join(output_folder, processing_request.page_uuid + '.logits.zip')

        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
        
        page_name = None

        for result in processing_request.results:
            ext = os.path.splitext(result.name)[1]
            datatype = magic.from_buffer(result.content, mime=True)
            if datatype.split('/')[0] == 'image':  # recognize image
                page_name = result.name
                continue
            try:
                with FileLock(lock_file=lock_path, timeout=20):
                    if ext == '.xml':
                        with zipfile.ZipFile(zip_path, 'a', zipfile.ZIP_DEFLATED) as zipf:
                            zipf.writestr(result.name, result.content.decode())
                    if ext == '.logits':
                        with zipfile.ZipFile(logits_path, 'a', zipfile.ZIP_DEFLATED) as zipf:
                            zipf.writestr(result.name, result.content)
            except Timeout as e:
                self.logger.error('Failed to write XML file to zip file for page {page}'.format(page = processing_request.page_uuid))
                raise
        
        # get status
        status = db_model.PageState.PROCESSED
        for log in processing_request.logs:
            if log.status != 'OK':
                status == db_model.PageState.PROCESSING_FAILED
        
        # calculate engine version - needs changes in zookeeper
        engine_version = None
        if status == db_model.PageState.PROCESSED:
            # TODO
            # calculate score - from xml
            score = None
            # change state to processed in database, save score, save engine version
            self.db_change_page_to_processed(
                page_id=processing_request.page_uuid,
                score=score,
                engine_version=engine_version
            )
        else:
            self.db_change_page_to_failed(
                page_id=processing_request.page_uuid,
                fail_type='PROCESSING_FAILED',
                traceback=processing_request.logs[-1].log,  # save log
                engine_version=engine_version
            )
        # remove image from config['UPLOAD_IMAGES_FOLDER']
        image_path = os.path.join(self.config['APIConfig']['UPLOAD_IMAGES_FOLDER'], processing_request.page_uuid, '{}'.format(page_name))
        if os.path.exists(image_path):
            os.unlink(image_path)

        # acknowledge the message
        self.mq_channel.basic_ack(delivery_tag=method.delivery_tag)

        # reduce number of messages in processing by 1
        try:
            self.request_count_lock.release()
        except ValueError as e:
            self.logger.warning('More processing requests were received than send, there might be some errors in processing or synchronization!')

    def mq_receive_results(self, queue):
        """
        Receives results from MQ server from given queue.
        :param queue: queue from where results are downloaded
        """
        self.mq_connect()
        
        self.mq_channel.basic_consume(
                queue,
                self._mq_receive_result_callback,
                auto_ack=False
            )
        while True:
            try:
                self.mq_channel.start_consuming()
            except KeyboardInterrupt:
                self.logger.info('Stoped result receiving.')
                break
            except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed) as e:
                self.logger.error('Result receiving failed!')
                self.logger.debug('Received error: {}'.format(e))
                self.mq_connect()
            except Exception as e:
                self.logger.error('Result receiving failed!')
                self.logger.debug('Received error: {}'.format(e))
                break

    def mq_send_request(self, page, engine, output_queue):
        """
        Send request with page for processing.
        :param page: page to include in request
        :param engine: engine to use for processing
        :param output_queue: api output queue name
        :return: timestamp when request was send
        """
        image_path = os.path.join(self.config['APIConfig']['UPLOAD_IMAGES_FOLDER'], str(page.request_id), page.name)
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
        request.uuid = page.request_id
        request.page_uuid = page.id
        request.priority = 0  # TODO - enable 0/1 priority
        for stage in engine.pipeline.split(','):
            request.processing_stages.append(stage.strip())
        request.processing_stages.append(output_queue)  # add output queue
        try:
            with open(image_path, 'rb') as image:
                img = request.results.add()
                img.name = page.name
                img.content = image.read()
        except OSError as e:
            raise OSError('Image not found!')
        
        Timestamp.FromDatetime(request.start_time, page.processing_timestamp)

        try:
            self.mq_channel.basic_publish('', engine.pipeline.split(',')[0].strip(), request.SerializeToString(),
                properties=pika.BasicProperties(delivery_mode=2, priority=request.priority),
                mandatory=True
            )
        except pika.exceptions.UnroutableError as e:
            logger.error('Request was rejected by the MQ broker!')
            logger.debug('Received error: {}'.format(e))
            raise
        except pika.exceptions.AMQPError as e:
            logger.error('Request was not confirmed by the MQ broker!')
            logger.debug('Received error: {}'.format(e))
            raise

    def mq_upload_requests(self, output_queue):
        """
        Periodically uploades waiting processing requests to MQ.
        :param output_queue: name of output queue where api receiver can pickup complete tasks
        """
        try:
            while True:
                self.mq_connect()

                if not self.db_session:
                    self.db_session = self.worker_adapter.db_create_session()

                # wait until new processing requests can be uploaded to MQ
                self.request_count_lock.acquire()
                try:
                    self.request_count_lock.release()
                except ValueError as e:
                    self.logger.warning('More processing requests were received than send, there might be some errors in processing or synchronization!')

                # fetch new pages from database
                waiting_pages = self.db_session.query(db_model.Page).join(db_model.Request).join(db_model.ApiKey) \
                                .filter(db_model.ApiKey.suspension == False) \
                                .filter(db_model.Page.state == db_model.PageState.WAITING) \
                                .all()
                if not waiting_pages:
                    # wait for some time if no pages could be uploaded (to prevent agressive fetching from db)
                    self.db_fetch_interval = self.db_fetch_interval * 2
                    if self.db_fetch_interval > int(self.config['AdapterConfig']['MAX_DB_FETCH_INTERVAL']):
                        self.db_fetch_interval = int(self.config['AdapterConfig']['MAX_DB_FETCH_INTERVAL'])
                    time.sleep(self.db_fetch_interval)
                else:
                    self.db_fetch_interval = int(self.config['AdapterConfig']['MIN_DB_FETCH_INTERVAL'])
                    
                    # upload as many requests as possible
                    for page in waiting_pages:
                        # exit if no more messages can be uploaded
                        if not self.request_count_lock.acquire(blocking=False):
                            break

                        # Add engine (pipeline)
                        engine = self.db_session.query(db_model.Engine).join(db_model.Request).filter(db_model.Request.id == page.request_id).first()

                        # update page
                        page.state = db_model.PageState.PROCESSING
                        page.processing_timestamp = datetime.datetime.now(datetime.timezone.utc)
                        self.db_session.commit()

                        # upload request to MQ
                        try:
                            self.mq_send_request(page, engine, output_queue)
                        except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed) as e:
                            self.logger.error('Failed to upload processing request, MQ server disconected!')
                            self.logger.debug('Received error: {}'.format(e))
                            break
                        except (ConnectionError, OSError) as e:
                            self.logger.error(f'Failed to upload page {page.id} to MQ!')
                            self.logger.debug(f'Received error: {e}')
                            self.db_change_page_to_failed(
                                page_id = page.id,
                                fail_type = 'NOT_FOUND',
                                traceback = f'{e}',
                                engine_version = None
                            )
                        except Exception as e:
                            self.logger.error(f'Failed to upload page {page.id} to MQ!')
                            self.logger.debug(f'Received error: {e}')
                            self.db_change_page_to_failed(
                                page_id = page.id,
                                fail_type = 'PROCESSING_FAILED',
                                traceback = f'{e}',
                                engine_version = None
                            )
        except KeyboardInterrupt:
            self.logger.info('Stopped request uploading!')

class WorkerAdapter(ZkClient, DBClient):
    def __init__(self, config, logger = logging.getLogger(__name__)):
        """
        Initializes worker adapter.
        :param config: API config object instance
        :param logger: logger to use for logging
        """
        # init ZkClient
        super(WorkerAdapter, self).__init__(
            zookeeper_servers=config['AdapterConfig']['ZOOKEEPER_SERVERS'],
            username=config['AdapterConfig']['USERNAME'],
            password=config['AdapterConfig']['PASSWORD'],
            ca_cert=config['AdapterConfig']['CA_CERT'],
            logger=logger
        )
        super(ZkClient, self).__init__(database_url=config['DBConfig']['database_url'])

        # mq servers
        self.mq_servers_lock = threading.Lock()
        self.mq_servers = []

        # config
        self.config = config

    def __del__(self):
        # del ZkClient
        super().__del__()
    
    ### ZK ###
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
    
    ### STATE ###
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

    ### MAIN ###
    def run(self):
        """
        Starts receiving and publishing requests from/to MQ.
        """
        # get list of MQ servers
        self.zk_connect()
        self.register_update_mq_server_callback()
        
        # initialize shared request counter
        request_count_lock = threading.BoundedSemaphore(value=int(self.config['AdapterConfig']['MAX_REQUEST_COUNT']))
        
        # get count of requests in processing (failure recovery)
        self.db_connect()
        db_session = self.db_create_session()
        request_count = len(
            db_session.query(db_model.Page).join(db_model.Request).join(db_model.ApiKey) \
            .filter(db_model.ApiKey.suspension == False) \
            .filter(db_model.Page.state == db_model.PageState.PROCESSING) \
            .all()
        )
        db_session.close()

        for i in range(request_count):
            if not request_count_lock.acquire(blocking=False):
                break
        
        # create publisher and receiver
        publisher_client = WorkerAdapterMQClient(self, self.config, request_count_lock, self.logger)
        receiver_client = WorkerAdapterMQClient(self, self.config, request_count_lock, self.logger)

        # DEBUG - single threaded
        #publisher_client.mq_upload_requests('api_out')
        #receiver_client.mq_receive_results('api_out')
        #return

        # run publisher and receiver
        publisher = threading.Thread(target=publisher_client.mq_upload_requests, args=('api_out',), name='publisher')
        receiver = threading.Thread(target=receiver_client.mq_receive_results, args=('api_out',), name='receiver')
        publisher.start()
        receiver.start()

        # wait for shutdown
        try:
            publisher.join()
            receiver.join()
        except KeyboardInterrupt:
            publisher.join()
            receiver.join()
        

def main():
    app_config = configparser.ConfigParser()
    app_config.read('./worker_adapter_config.ini')
    worker_adapter = WorkerAdapter(
        config=app_config,
        logger=logging.getLogger(__name__)
    )
    worker_adapter.run()
    return 0
    
if __name__ == "__main__":
    sys.exit(main())