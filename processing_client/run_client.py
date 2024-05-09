import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

import os
import re
import cv2
import sys
import time
import zipfile
import requests
import argparse
import traceback
import numpy as np
import configparser
from urllib.request import Request, urlopen
from pathlib import Path
import torch

from pero_ocr.document_ocr.page_parser import PageParser
from pero_ocr.core.layout import PageLayout, create_ocr_processing_element
from pero_ocr.core.arabic_helper import ArabicHelper

from helper import join_url


def get_args():
    """
    method for parsing of arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", required=True, help="Config path.")
    parser.add_argument("-a", "--api-key", help="API key.")
    parser.add_argument("-e", "--preferred-engine", dest="engine", help="Preferred engine ID.")
    parser.add_argument("--test-mode", action="store_true", help="Doesn't send results to server.")
    parser.add_argument("--test-path", default='./', help="Path to store files in test mode.")
    parser.add_argument("--exit-on-done", action="store_true", help="Exit when no more data from server.")
    parser.add_argument("--time-limit", default=-1, type=float, help="Exit when runing longer than time-limit hours.")
    parser.add_argument("--page-pooling-delay", default=2, type=float, help="Delay between queries for processing.")
    parser.add_argument("--min-confidence", default=0.66, type=float,
                        help="Lines with lower confidence will be discarded.")
    parser.add_argument("--reset-engine-timeout", default=1.0/60, type=float,
                        help="Time in hours. How often should the model switch between OCR engines. The engine remains the same as long as there are pages waiting to be processed by the current engine.")

    args = parser.parse_args()

    return args


def timeit(func):
    def timed(*args, **kwargs):
        ts = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - ts
        logging.info(f'{func.__name__} {elapsed:.3f} s')
        return result
    return timed


def try_to_get_local_engine_version(session, config, engine_id):
    url = join_url(config['SERVER']['base_url'],
                   'latest_engine_version',
                   str(engine_id))
    r = session.get(url)
    if r.status_code != 200:
        logging.error(f'Request {url} failed with code {r.status_code}.')
        return None

    response = r.json()
    if response['status'] != 'success':
        logging.error(f'Request {url} returned status {response["status"]}.')
        return None

    filename = response['filename']
    if os.path.exists(os.path.join(config["SETTINGS"]['engines_path'], filename[:-4])):
        return filename

    return None


def load_engine(config, filename, engine_name, engine_version):
    t1 = time.time()
    engine_config = configparser.ConfigParser()
    engine_config.read(os.path.join(config["SETTINGS"]['engines_path'], filename[:-4], 'config.ini'))
    page_parser = PageParser(engine_config,
                             config_path=os.path.dirname(os.path.join(config["SETTINGS"]['engines_path'],
                                                                      filename[:-4],
                                                                      'config.ini')))
    logging.info(f'Loded engine {engine_name} - {engine_version}. Time: {time.time() - t1}')
    return page_parser


@timeit
def get_engine(session, config, engine_id):
    logging.info(f'Starting engine {engine_id}.')

    page_parser = None
    filename = try_to_get_local_engine_version(session, config, engine_id)
    if filename:
        engine_name = filename[:-4].split('#')[0]
        engine_version = filename[:-4].split('#')[1]
        logging.warning(f'Loading previously downloaded engine version {filename}. ')
        try:
            page_parser = load_engine(config, filename, engine_name, engine_version)
        except KeyboardInterrupt:
            raise
        except:
            logging.warning(f'Failed to reload  {engine_name} - {engine_version} from {filename}.')
        else:
            return page_parser, engine_name, engine_version

    t1 = time.time()
    r = session.get(join_url(config['SERVER']['base_url'],
                             config['SERVER']['get_download_engine'],
                             str(engine_id)))

    d = r.headers['content-disposition']
    filename = re.findall("filename=(.+)", d)[0]
    engine_name = filename[:-4].split('#')[0]
    engine_version = filename[:-4].split('#')[1]
    if not os.path.exists(os.path.join(config["SETTINGS"]['engines_path'], filename[:-4])):
        os.mkdir(os.path.join(config["SETTINGS"]['engines_path'], filename[:-4]))

    with open(os.path.join(config["SETTINGS"]['engines_path'], filename[:-4], filename), 'wb') as f:
        f.write(r.content)
    with zipfile.ZipFile(os.path.join(config["SETTINGS"]['engines_path'], filename[:-4], filename), 'r') as f:
        f.extractall(os.path.join(config["SETTINGS"]['engines_path'], filename[:-4]))

    logging.info(f'Downloaded engine {engine_name} - {engine_version}. Time: {time.time()-t1}.')

    page_parser = load_engine(config, filename, engine_name, engine_version)

    return page_parser, engine_name, engine_version


def get_page_layout_text(page_layout):
    text = ""
    for line in page_layout.lines_iterator():
        text += "{}\n".format(line.transcription)
    return text


def get_score(page_layout):
    line_quantiles = []
    for line in page_layout.lines_iterator():
        if line.transcription_confidence is not None:
            line_quantiles.append(line.transcription_confidence)
    if not line_quantiles:
        return 1.0
    else:
        return np.quantile(line_quantiles, .50)


@timeit
def get_request(config, session):
    try:
        r = session.get(join_url(config['SERVER']['base_url'],
                                 config['SERVER']['get_processing_request'],
                                 config['SETTINGS']['preferred_engine']))
    except requests.exceptions.ConnectionError:
        status = 'failed'
    else:
        if r.status_code == 200:
            request = r.json()
            status = request['status']
        else:
            status = 'failed'

    if status != 'success':
        return None, None, None

    page_id = request['page_id']
    page_url = request['page_url']
    engine_id = request['engine_id']
    return page_id, page_url, engine_id


@timeit
def get_image(page_url, config, session):
    if config['SERVER']['base_url'] in page_url:
        r = session.get(page_url, stream=True)
        resp = r.raw
        encoded_image = resp.read()
    else:
        req = Request(page_url)
        req.add_header('User-Agent',
                       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11')
        req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
        encoded_image = urlopen(req).read()

    encoded_img = np.frombuffer(encoded_image, dtype=np.uint8)
    image = cv2.imdecode(encoded_img, flags= cv2.IMREAD_COLOR)
    return image


@timeit
def process_image(page_parser, image, page_id, args, engine_name, engine_version, arabic_helper):
    # Process image
    page_layout = PageLayout(id=page_id, page_size=(image.shape[0], image.shape[1]))
    page_layout = page_parser.process_page(image, page_layout)

    ocr_processing_element = create_ocr_processing_element(id="IdOcr",
                                                   software_creator_str="Project PERO",
                                                   software_name_str="{}".format(engine_name),
                                                   software_version_str="{}".format(engine_version),
                                                   processing_datetime=None)

    alto_xml = page_layout.to_altoxml_string(ocr_processing_element=ocr_processing_element,
                                             min_line_confidence=args.min_confidence)

    if args.min_confidence > 0:
        for region in page_layout.regions:
            region.lines = \
                [l for l in region.lines if
                 l.transcription_confidence and l.transcription_confidence > args.min_confidence]

    for line in page_layout.lines_iterator():
        if arabic_helper.is_arabic_line(line.transcription):
            line.transcription = arabic_helper.label_form_to_string(line.transcription)

    page_xml = page_layout.to_pagexml_string()
    text = get_page_layout_text(page_layout)

    return page_layout, page_xml, alto_xml, text

@timeit
def send_results(args, config, session, page_id, engine_version, page_layout, page_xml, alto_xml, text):
    if args.test_mode:
        with open(os.path.join(args.test_path, '{}_alto.xml'.format(page_id)), "w") as file:
            file.write(alto_xml)
        with open(os.path.join(args.test_path, '{}_page.xml'.format(page_id)), "w") as file:
            file.write(page_xml)
        with open(os.path.join(args.test_path, '{}.txt'.format(page_id)), "w") as file:
            file.write(text)
    else:
        headers = {'api-key': config['SETTINGS']['api_key'],
                   'engine-version': engine_version,
                   'score': str(get_score(page_layout))}
        session.post(join_url(config['SERVER']['base_url'], config['SERVER']['post_upload_results'], page_id),
                     files={'alto': ('{}_alto.xml'.format(page_id), alto_xml, 'text/plain'),
                            'page': ('{}_page.xml'.format(page_id), page_xml, 'text/plain'),
                            'txt': ('{}.txt'.format(page_id), text, 'text/plain')},
                     headers=headers)


def log_and_post_failure(session, config, engine_name,  engine_version, page_id, page_url, error_state):
    exception = traceback.format_exc()
    logging.error(f'Failed processing with engine {engine_name} - {engine_version} of page {page_id} - {page_url}. {exception}')

    headers = {'api-key': config['SETTINGS']['api_key'],
               'type': error_state,
               'engine-version': engine_version}
    session.post(
        join_url(config['SERVER']['base_url'], config['SERVER']['post_failed_processing'], page_id),
        data=exception.encode('utf-8'),
        headers=headers)

def main():
    args = get_args()

    if not torch.cuda.is_available():
        logging.error('Cuda is not available.')
        exit(-1)

    start_time = time.time()

    config = configparser.ConfigParser()
    config.read(args.config)

    Path(config['SETTINGS']['engines_path']).mkdir(parents=True, exist_ok=True)

    if args.api_key is not None:
        config["SETTINGS"]['api_key'] = args.api_key

    if args.engine is not None:
        config["SETTINGS"]['preferred_engine'] = args.preferred_engine

    arabic_helper = ArabicHelper()

    session = requests.Session()
    session.headers.update({'api-key': config['SETTINGS']['api_key']})

    page_parser = None
    engine_name = None
    engine_version = None
    loaded_engine_id = None
    last_engine_switch_time = time.time()

    while True:
        if args.time_limit > 0 and args.time_limit * 3600 < time.time() - start_time:
            logging.info(f'Stopping after reaching time limit. Running time {time.time() - start_time}s.')
            break

        if last_engine_switch_time + args.reset_engine_timeout * 3600 < time.time():
            config['SETTINGS']['preferred_engine'] = str(0)

        page_id, page_url, requested_engine_id = get_request(config, session)
        if not page_id:
            logging.info(f'No page to process.')
            if args.exit_on_done:
                break
            else:
                time.sleep(args.page_pooling_delay)
                continue

        logging.info(f'Have page {page_id} {page_url} {requested_engine_id}.')

        if loaded_engine_id != requested_engine_id:
            last_engine_switch_time = time.time()
            page_parser, engine_name, engine_version = get_engine(session, config, requested_engine_id)
            loaded_engine_id = requested_engine_id
            config['SETTINGS']['preferred_engine'] = str(loaded_engine_id)
            logging.info(f'Loaded engine {loaded_engine_id} - {engine_name}. Time {time.time() - last_engine_switch_time}s')

        try:
            error_state = 'INVALID_FILE'
            image = get_image(page_url, config, session)
            logging.info(f'Image {image.shape[1]}x{image.shape[0]}.')

            error_state = 'PROCESSING_FAILED'
            page_layout, page_xml, alto_xml, text = process_image(page_parser, image, page_id, args, engine_name, engine_version, arabic_helper)
            send_results(args, config, session, page_id, engine_version, page_layout, page_xml, alto_xml, text)
        except KeyboardInterrupt:
            traceback.print_exc()
            logging.info('Terminated by user.')
            sys.exit()
        except:
            logging.error(f'FAILED. Page: {page_id}, State {error_state}')
            log_and_post_failure(session, config, engine_name, engine_version, page_id, page_url, error_state)
            continue
        else:
            lines = 0
            chars = 0
            for line in page_layout.lines_iterator():
                lines += 1
                chars += len(line.transcription)
            logging.info(f'DONE Page: {page_id}, lines {lines}, chars {chars}.')

        torch.cuda.empty_cache()


if __name__ == '__main__':
    main()
