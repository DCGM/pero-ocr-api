#!/usr/bin/env python3

# Worker adapter - connects api to workers via Apache Zookeeper and RabbitMQ
# Sends processing requests to worker system and receives processed results

import worker_functions.connection_aux_functions as cf
import worker_functions.constants as constants
from worker_functions.zk_client import ZkClient
from worker_functions.mq_client import MQClient
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data

from pero_ocr.document_ocr.layout import PageLayout, create_ocr_processing_element

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
    

class WorkerAdapter(ZkClient, MQClient, DBClient):
    def __init__(self, config, logger = logging.getLogger(__name__)):
        """
        Initializes worker adapter.
        :param config: API config object instance
        :param logger: logger to use for logging
        """
        # init ZkClient
        super(WorkerAdapter, self).__init__(
            zookeeper_servers=config['Adapter']['ZOOKEEPER_SERVERS'],
            username=config['Adapter']['USERNAME'],
            password=config['Adapter']['PASSWORD'],
            ca_cert=config['Adapter']['CA_CERT'],
            logger=logger
        )
        # init MQClient
        super(ZkClient, self).__init__(
            mq_servers=[],
            username=config['Adapter']['USERNAME'],
            password=config['Adapter']['PASSWORD'],
            ca_cert=config['Adapter']['CA_CERT'],
            logger=logger
        )
        # init DBClient
        super(MQClient, self).__init__(
            database_url=config['DB']['database_url']
        )

        # mq servers
        self.mq_servers_lock = threading.Lock()
        self.mq_servers = []

        # config
        self.config = config

        # current session
        self.db_session = None

    def __del__(self):
        # del ZkClient
        super(WorkerAdapter, self).__del__()
        super(ZkClient, self).__del__()
    
    ### MAIL ###
    def send_mail(subject, body):
        """
        Sends mail with given subject and body to addresses specified in cofiguration.
        :param subject: mail subject
        :param body: mail body
        :nothrow
        """
        try:
            if self.config['Mail']['NOTIFICATION_ADDRESSES']:
                send_mail(
                        subject=subject,
                        body=body.replace("\n", "<br>"),
                        sender=('PERO OCR - API BOT', self.config['Mail']['USERNAME']),
                        password=self.config['Mail']['PASSWORD'],
                        recipients=[ address.strip() for address in self.config['Mail']['NOTIFICATION_ADDRESSES'].split(',')],
                        host=self.config['Mail']['SERVER']
                    )
        except Exception:
            self.logger.error('Failed to send notification email!')
            self.logger.debug(f'Received error:\n{traceback.format_exc()}')
    
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
    
    ### DB ###
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
    
    ### MQ ###
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
                    .format(n=int(self.config['Adapter']['CONNECTION_RETRY_INTERVAL']))
                )
                time.sleep(int(self.config['Adapter']['CONNECTION_RETRY_INTERVAL']))

            self.mq_servers = self.get_mq_servers()
            super().mq_connect()

            retry_count += 1
            if max_retry and retry_count == max_retry:
                break

        if not (self.mq_connection and self.mq_connection.is_open and self.mq_channel and self.mq_channel.is_open):
            raise ConnectionError('Failed to connect to MQ servers!')
        
        self.mq_channel.confirm_delivery()
    
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
        # save results to db and disk
        if not self.db_session:
            self.db_session = self.db_create_session()

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
        status = db_model.PageState.PROCESSED
        engine_version = []
        for log in processing_request.logs:
            engine_version.append(f'{log.stage}: {log.version}')
            if log.status != 'OK':
                status == db_model.PageState.PROCESSING_FAILED
        engine_version = ', '.join(engine_version)

        if status == db_model.PageState.PROCESSED:  # processing OK
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

            page_layout = PageLayout()
            page_layout.from_pagexml_string(page_xml.content)
            page_layout.load_logits(page_logits.content)
            
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
            self.db_change_page_to_processed(
                page_id=processing_request.page_uuid,
                score=score,
                engine_version=engine_version
            )
            # remove image from config['UPLOAD_IMAGES_FOLDER']
            if page_img.name:
                image_path = os.path.join(self.config['API']['UPLOAD_IMAGES_FOLDER'], processing_request.page_uuid, '{}'.format(page_img.name))
                if os.path.exists(image_path):
                    os.unlink(image_path)
        
        else:  # processing failed
            self.db_change_page_to_failed(
                page_id=processing_request.page_uuid,
                fail_type=db_model.PageState.PROCESSING_FAILED,
                traceback=processing_request.logs[-1].log,  # save log
                engine_version=engine_version
            )
            # send email notification
            self.send_mail(
                subject='API Bot - PROCESSING FAILED',
                body=processing_request.logs[-1].log
            )

        # acknowledge the message
        self.mq_channel.basic_ack(delivery_tag=method.delivery_tag)

    def mq_receive_results(self, queue):
        """
        Receives results from MQ server from given queue.
        :param queue: queue from where results are downloaded
        """
        while True:
            # connection init
            self.mq_connect()
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
                self.logger.info('Stoped result receiving.')
                break
            except pika.exceptions.AMQPError as e:
                # connection failed - continue / recover
                self.logger.error('Result receiving failed!')
                self.logger.debug('Received error: {}'.format(e))
            except Exception as e:
                # unrecoverable error - exit
                self.logger.error('Result receiving failed!')
                self.logger.debug('Received error:\n{}'.format(traceback.format_exc()))
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
        request.uuid = page.request_id
        request.page_uuid = page.id
        request.priority = 0  # TODO - enable 0/1 priority
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
        try:
            while True:
                self.mq_connect()

                if not self.db_session:
                    self.db_session = self.db_create_session()
                
                # get maximum number of requests that can be uploaded
                # (maximum number of requests - number of requests in processing)
                upload_request_count = int(self.config['Adapter']['MAX_REQUEST_COUNT']) - len(
                    self.db_session.query(db_model.Page).join(db_model.Request).join(db_model.ApiKey) \
                    .filter(db_model.ApiKey.suspension == False) \
                    .filter(db_model.Page.state == db_model.PageState.PROCESSING) \
                    .all()
                )

                # fetch new pages from database
                waiting_pages = self.db_session.query(db_model.Page).join(db_model.Request).join(db_model.ApiKey) \
                                .filter(db_model.ApiKey.suspension == False) \
                                .filter(db_model.Page.state == db_model.PageState.WAITING) \
                                .all()
                if not waiting_pages or upload_request_count <= 0:
                    # wait for some time if no pages could be uploaded (to prevent agressive fetching from db)
                    time.sleep(int(self.config['Adapter']['DB_FETCH_INTERVAL']))
                    continue

                # upload as many requests as possible
                for page, i in zip(waiting_pages, range(upload_request_count)):
                    # Add engine (pipeline)
                    engine = self.db_session.query(db_model.Engine).join(db_model.Request).filter(db_model.Request.id == page.request_id).first()

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
                        self.send_mail(
                            subject='API Bot - Request upload failed!',
                            body=f'{e}'
                        )
                        time.sleep(int(self.config['Adapter']['DB_FETCH_INTERVAL']))
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
                        self.db_change_page_to_failed(
                            page_id = page.id,
                            fail_type = db_model.PageState.NOT_FOUND,
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
                        page.state = db_model.PageState.PROCESSING
                        page.processing_timestamp = timestamp
                        self.db_session.commit()
                
        except KeyboardInterrupt:
            self.logger.info('Stopped request uploading!')

    ### MAIN ###
    def run(self, publisher=False):
        """
        Starts receiving or publishing requests from/to MQ.
        :param publisher: Determines if adapter is publisher or receiver (default)
        :param queue: Output queue where adapter can pickup processing results
        :return: 0 - success / Raises exception otherwise
        """
        # get list of MQ servers
        self.zk_connect()
        self.register_update_mq_server_callback()

        # connect DB
        self.db_connect()

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
    return parser.parse_args()

def main():
    args = get_args()

    app_config = configparser.ConfigParser()
    app_config.read(args.config)

    worker_adapter = WorkerAdapter(
        config=app_config,
        logger=logging.getLogger(__name__)
    )

    worker_adapter.run(publisher=args.publisher)

    return 0
    
if __name__ == "__main__":
    sys.exit(main())