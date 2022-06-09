# Based on: https://app.swaggerhub.com/apis-docs/LachubCz/PERO-API/1.0.1#/ by Michal Hradiš.
# For running the script you need to get the api key.
# First enter the link with data.
# Second enter the resulting format (alto, txt, page)

import os
import sys
import requests
import argparse
from collections import defaultdict


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--api-url', type=str, help='API URL', required=True)
    parser.add_argument('-a', '--api-key', type=str, help='API key', required=True)
    parser.add_argument('-o', '--output-path', type=str, help='Where download result files.', required=True)
    parser.add_argument('-i', '--request-id', type=str, help='Id provided by the API when the request was created.', required=True)
    parser.add_argument('--alto', action='store_true', help='Download results in ALTO XML format.')
    parser.add_argument('--page', action='store_true', help='Download results in PAGE XML format.')
    parser.add_argument('--txt', action='store_true', help='Download results in plain text format.')
    args = parser.parse_args()
    return args


def get_request_status(server_url, api_key, request_id):
    url = f"{server_url}/request_status/{request_id}"
    r = requests.get(url, headers={"api-key": api_key})

    if r.status_code == 401:
        print(f'ERROR: Request with id {request_id} does not belong to this API key.')
        exit(-1)
    if r.status_code == 404:
        print(f'ERROR: Request with id {request_id} does not exist.')
        exit(-1)
    if r.status_code != 200:
        print(f'ERROR: Request returned with unexpected status code: {r.status_code}')
        print(r.text)
        exit(-1)

    response = r.json()

    if response['status'] != "success":
        print(f'ERROR: Unexpected request query status: {response["status"]}')
        print(response)
        exit(-1)

    return response['request_status']


def download_results(page_name, session, server_url, api_key, request_id, output_path, alto, page, txt):
    path = os.path.join(output_path, page_name)
    requested_formats = []
    if alto:
        requested_formats.append('alto')
    if page:
        requested_formats.append('page')
    if txt:
        requested_formats.append('txt')

    for file_format in requested_formats:
        file_path = f'{path}.{file_format}'
        if os.path.exists(file_path):
            continue

        url = f"{server_url}/download_results/{request_id}/{page_name}/{file_format}"
        r = session.get(url, headers={"api-key": api_key})
        if r.status_code == 400:
            print(f'ERROR: Unknown export format: {file_format}')
            continue
        if r.status_code == 401:
            print(f'ERROR: Request with id {request_id} does not belong to this API key.')
            continue
        if r.status_code == 404:
            print(f'ERROR: Request with id {request_id} does not exist.')
            continue
        if r.status_code != 200:
            print(f'ERROR: Request returned with unexpected status code: {r.status_code}')
            print(r.text)
            continue

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(r.text)


def main():
    args = parse_arguments()
    page_status = get_request_status(args.api_url, args.api_key, args.request_id)

    os.makedirs(args.output_path, exist_ok=True)

    session = requests.Session()

    state_counts = defaultdict(int)
    for page_name in sorted(page_status):
        if page_status[page_name]['state'] == 'PROCESSED':
            print(page_name, page_status[page_name]['state'], page_status[page_name]['quality'])
            download_results(page_name, session, args.api_url, args.api_key, args.request_id, args.output_path, args.alto, args.page, args.txt)
        else:
            print(page_name, page_status[page_name]['state'])

        state_counts[page_status[page_name]['state']] += 1

    print('SUMMARY:')
    for state in state_counts:
        print(state, state_counts[state])

    if state_counts['WAITING'] + state_counts['PROCESSING'] == 0:
        print('ALL PAGES DONE')
        exit(0)
    else:
        exit(1)


if __name__ == "__main__":
    main()
