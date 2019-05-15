import argparse
import iso8601
import json
import logging
import mpu
import requests
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

log = logging.getLogger('kmt2es')

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'cookie': ''  # value will be set in main method
}

def _request_tours(user_id, full_index):
    entries_per_page = 100 if full_index == True else 10
    tours = []
    page_index = 0
    page = _request_tour_page(user_id, page_index, entries_per_page)
    tours += page['_embedded']['tours']
    log.debug("Found {} tour pages in total".format(page['page']['totalPages']))
    if full_index:
        while page['page']['number'] < (page['page']['totalPages'] - 1):
            page_index += 1
            page = _request_tour_page(user_id, page_index, entries_per_page)
            tours += page['_embedded']['tours']
    return tours


def _request_tour_page(user_id, page_index, entries_per_page=10):
    url = 'https://www.komoot.de/api/v007/users/{user_id:}/tours/?page={page:}&limit={limit:}'.format(user_id=user_id, page=page_index, limit=entries_per_page)
    log.debug("Requesting data from {}".format(url))
    r = requests.get(url, headers=headers)
    if r.status_code >= 400:
        raise RuntimeError("Request failed with status code {}.".format(r.status_code))
    return json.loads(r.text)

def _request_coordinates(tour_id):
    r = requests.get('http://api.komoot.de/v007/tours/{}/coordinates'.format(tour_id), headers=headers)
    if r.status_code >= 400:
        raise RuntimeError("Request failed with status code {}.".format(r.status_code))
    return json.loads(r.text)

def _transform_coordinates(coordinates, start_date, tour_id, tour_sport, cli_args):
    """
    Calculate the timestamp by adding deltas to the initial timestamp of the
    tour. Then transform coordinates entries in a native format that
    ElasticSearch can work with. This allows to index coordinates in a bulk
    operation.
    """
    dt_start = iso8601.parse_date(start_date)
    rows = []
    prev_row = None
    for i, row in enumerate(coordinates['items']):
        delta = timedelta(microseconds=row['t']) * 1000
        geopoint = [row['lng'], row['lat']]
        distance = 0.0 if prev_row is None else mpu.haversine_distance((prev_row['lat'], prev_row['lng']), (row['lat'], row['lng']))
        time_delta_in_s = 0.0 if prev_row is None else (row['t'] - prev_row['t']) / 1000
        speed = 0.0 if prev_row is None or time_delta_in_s == 0.0 else distance / time_delta_in_s
        rows.append({ 
            '_index': cli_args.es_index_format_coordinates.format(year=dt_start.year, month=dt_start.month), 
            '_type': 'coordinate',
            '_id': '{}_{}'.format(tour_id, i), 
            'tour_id': tour_id, 
            'date': (dt_start + delta).isoformat(), 
            'lat': row['lat'], 
            'lng': row['lng'], 
            'geopoint': geopoint,
            'distance': distance,
            'speed': speed,
            'alt': float(row['alt']),
            'sport': tour_sport,
        })
        prev_row = row
    return rows

def _send_to_elasticsearch(es, tours, cli_args):
    for i, row in enumerate(tours):
        # Limit results to recorded tours only
        if row['type'] != 'tour_recorded':
            continue

        tour_id = row['id']

        dt = iso8601.parse_date(row['date'])
        _create_elasticsearch_index(es, dt, cli_args)

        res = es.index(index=cli_args.es_index_format_tour.format(year=dt.year, month=dt.month, day=dt.day), id=tour_id, body=row)
        coordinates = _request_coordinates(tour_id)
        coordinates = _transform_coordinates(coordinates, row['date'], tour_id, row['sport'], cli_args)
        res = bulk(es, coordinates, chunk_size=1000, request_timeout=200)
        log.info("Imported tour id {}".format(tour_id))

def _create_elasticsearch_index(es, dt, cli_args):
    es.indices.create(index=cli_args.es_index_format_tour.format(year=dt.year, month=dt.month, day=dt.day), ignore=400)
    mappings = '''
    {
        "mappings": {
            "coordinate": {
                "properties": {
                    "geopoint": {
                        "type": "geo_point"
                    }
                }
            }
        }
    }'''
    es.indices.create(index=cli_args.es_index_format_coordinates.format(year=dt.year, month=dt.month, day=dt.day), ignore=400, body=mappings)

def main(cli_args):
    logging.basicConfig(level=cli_args.log_level.upper())

    es_args = {
        'hosts': [cli_args.es_host]
    }
    if cli_args.es_http_auth:
        es_args['http_auth'] = cli_args.es_http_auth
    es = Elasticsearch(**es_args)
    log.info(es.info())

    log.info("Use elasticsearch index format for tours: " + cli_args.es_index_format_tour)
    log.info("Use elasticsearch index format for coordinates: " + cli_args.es_index_format_coordinates)

    # Update cookie header that will be used to verify
    headers['cookie'] = cli_args.cookie

    tours_data = _request_tours(user_id=cli_args.user_id, full_index=cli_args.full_index)
    log.info("Index latest {} tours".format(len(tours_data)))
    _send_to_elasticsearch(es, tours_data, cli_args)

if __name__ == "__main__":
    es_index_tour_default = 'komoot-tour-{year:02d}-{month:02d}'
    es_index_coordinates_default = 'komoot-coordinates-{year:02d}-{month:02d}'

    parser = argparse.ArgumentParser(description='Import komoot tours into elasticsearch database.')
    parser.add_argument("-u", "--user-id", 
        dest="user_id", help="Komoot user id", required=True)
    parser.add_argument("--elasticsearch-host", 
        dest="es_host", help="Hostname of the ElasticSearch instance, e.g. \"http://localhost:9200\"", required=True)
    parser.add_argument("--elasticsearch-http-auth", 
        dest="es_http_auth", help="HTTP authentication for ElasticSearch instance. This should be left empty if no authentication is set up.", default=None)
    parser.add_argument("--full-index", 
        dest="full_index", 
        help="Indexes all recorded tours for the given user. Might use significantly more time, depending on the number of recoded tours. Otherwise only the latest 10 tours are indexed.", 
        action="store_true"
    )
    parser.add_argument("-c", "--cookie", 
        dest="cookie", 
        help="Cookie value of a valid session (used for authentication).", 
        required=True
    )
    parser.add_argument("-l", "--log", 
        dest="log_level", 
        choices=['debug', 'info', 'warning', 'error', 'critical'], 
        help="Set the logging level", 
        default='error'
    )
    parser.add_argument("--elasticsearch-index-format-tour", 
        dest="es_index_format_tour", 
        help="Set elasticsearch index format for komoot tours. Use year, month and day variables to set rollover index if needed.", 
        default=es_index_tour_default
    )
    parser.add_argument("--elasticsearch-index-format-coordinates", 
        dest="es_index_format_coordinates", 
        help="Set elasticsearch index format for coordinates of komoot tours. Use year, month and day variables to set rollover index if needed.", 
        default=es_index_coordinates_default
    )

    cli_args = parser.parse_args()

    main(cli_args)
