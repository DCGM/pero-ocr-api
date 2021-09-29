import os
import re
import cv2
import sys
import time
import socket
import zipfile
import requests
import argparse
import traceback
import numpy as np
import configparser
import logging
from urllib.request import Request, urlopen
from pathlib import Path

from pero_ocr.document_ocr.page_parser import PageParser
from pero_ocr.document_ocr.layout import PageLayout, create_ocr_processing_element
from pero_ocr.document_ocr.arabic_helper import ArabicHelper

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
    parser.add_argument("--min-confidence", default=0.66, type=float,
                        help="Lines with lower confidence will be discarded.")

    args = parser.parse_args()

    return args


def get_engine(session, config, engine_id):
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

    engine_config = configparser.ConfigParser()
    engine_config.read(os.path.join(config["SETTINGS"]['engines_path'], filename[:-4], 'config.ini'))
    page_parser = PageParser(engine_config,
                             config_path=os.path.dirname(os.path.join(config["SETTINGS"]['engines_path'],
                                                                      filename[:-4],
                                                                      'config.ini')))
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

def post_error()

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


    def get_image():
        try:
            req = Request(page_url)
            req.add_header('User-Agent',
                           'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11')
            req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
            if config['SERVER']['base_url'] in page_url:
                req.add_header('api-key', config['SETTINGS']['api_key'])
            page = urlopen(req).read()
        except KeyboardInterrupt:
            traceback.print_exc()
            print('Terminated by user.')
            sys.exit()
        except:
            exception = traceback.format_exc()
            headers = {'api-key': config['SETTINGS']['api_key'],
                       'type': 'NOT_FOUND',
                       'engine-version': engine_version}
            session.post(
                join_url(config['SERVER']['base_url'], config['SERVER']['post_failed_processing'], page_id),
                data=exception.encode('utf-8'),
                headers=headers)
            continue

        # Decode image
        # Download image from url.
        try:
            encoded_img = np.frombuffer(page, dtype=np.uint8)
            image = cv2.imdecode(encoded_img, flags=cv2.IMREAD_ANYCOLOR)
            if len(image.shape) == 2:
                image = np.stack([image, image, image], axis=2)
        except KeyboardInterrupt:
            traceback.print_exc()
            print('Terminated by user.')
            sys.exit()
        except:
            exception = traceback.format_exc()
            headers = {'api-key': config['SETTINGS']['api_key'],
                       'type': 'INVALID_FILE',
                       'engine-version': engine_version}
            session.post(
                join_url(config['SERVER']['base_url'], config['SERVER']['post_failed_processing'], page_id),
                data=exception,
                headers=headers)
            continue

        # Process image
        try:
            page_layout = PageLayout(id=page_id, page_size=(image.shape[0], image.shape[1]))
            page_layout = page_parser.process_page(image, page_layout)

        except KeyboardInterrupt:
            traceback.print_exc()
            print('Terminated by user.')
            sys.exit()
        except:
            exception = traceback.format_exc()
            headers = {'api-key': config['SETTINGS']['api_key'],
                       'type': 'PROCESSING_FAILED',
                       'engine-version': engine_version,
                       'hostname': socket.gethostname(),
                       'ip-address': socket.gethostbyname(socket.gethostname())}
            session.post(
                join_url(config['SERVER']['base_url'], config['SERVER']['post_failed_processing'], page_id),
                data=exception,
                headers=headers)
            continue
        else:
            ocr_processing = create_ocr_processing_element(id="IdOcr",
                                                           software_creator_str="Project PERO",
                                                           software_name_str="{}".format(engine_name),
                                                           software_version_str="{}".format(engine_version),
                                                           processing_datetime=None)

            alto_xml = page_layout.to_altoxml_string(ocr_processing=ocr_processing,
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

    else:
        if args.exit_on_done:
            break
        time.sleep(10)


def main():
    args = get_args()

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

    while True:
        if args.time_limit > 0 and args.time_limit * 3600 < time.time() - start_time:
            logging.info(f'Stopping after reaching time limit. Running time {time.time() - start_time}s.')
            break

        page_id, page_url, engine_id = get_request(config, session)
        if not page_id:
            logging.debug(f'No page to process.')
            time.sleep(2)
            continue

        logging.debug(f'Have page {page_id} {page_url} {engine_id}.')

        if loaded_engine_id != engine_id:
            page_parser, engine_name, engine_version = get_engine(config, engine_id)
            loaded_engine_id = engine_id
            logging.info(f'Loaded engine {engine_id} - {engine_name}.')





if __name__ == '__main__':
    main()
