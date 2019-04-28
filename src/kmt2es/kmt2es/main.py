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

ES_INDEX_TOUR = 'komoot-tour-{year:02d}-{month:02d}'
ES_INDEX_COORDINATES = 'komoot-coordinates-{year:02d}-{month:02d}'

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

def _transform_coordinates(coordinates, start_date, tour_id, tour_sport):
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
            '_index': ES_INDEX_COORDINATES.format(year=dt_start.year, month=dt_start.month), 
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

def _send_to_elasticsearch(es, tours):
    for i, row in enumerate(tours):
        # Limit results to recorded tours only
        if row['type'] != 'tour_recorded':
            continue

        tour_id = row['id']

        dt = iso8601.parse_date(row['date'])
        _create_elasticsearch_index(es, dt)

        res = es.index(index=ES_INDEX_TOUR.format(year=dt.year, month=dt.month), doc_type='tour', id=tour_id, body=row)
        coordinates = _request_coordinates(tour_id)
        coordinates = _transform_coordinates(coordinates, row['date'], tour_id, row['sport'])
        res = bulk(es, coordinates, chunk_size=1000, request_timeout=200)
        log.info("Imported tour id {}".format(tour_id))

def _create_elasticsearch_index(es, dt):
    es.indices.create(index=ES_INDEX_TOUR.format(year=dt.year, month=dt.month), ignore=400)
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
    es.indices.create(index=ES_INDEX_COORDINATES.format(year=dt.year, month=dt.month), ignore=400, body=mappings)

def main(args):
    logging.basicConfig(level=args.log_level.upper())

    es_args = {
        'hosts': [args.elasticsearch_host]
    }
    if args.elasticsearch_http_auth:
        es_args['http_auth'] = args.elasticsearch_http_auth
    es = Elasticsearch(**es_args)
    log.info(es.info())

    log.info("Use elasticsearch index format for tours: " + ES_INDEX_TOUR)
    log.info("Use elasticsearch index format for coordinates: " + ES_INDEX_COORDINATES)

    # Update cookie header that will be used to verify
    headers['cookie'] = args.cookie

    tours_data = _request_tours(user_id=args.user_id, full_index=args.full_index)
    log.info("Index latest {} tours".format(len(tours_data)))
    _send_to_elasticsearch(es, tours_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import komoot tours into elasticsearch database.')
    parser.add_argument("-u", "--user-id", dest="user_id", help="Komoot user id", required=True)
    parser.add_argument("--elasticsearch-host", dest="elasticsearch_host", help="Hostname of the ElasticSearch instance", required=True)
    parser.add_argument("--elasticsearch-http-auth", dest="elasticsearch_http_auth", help="HTTP Authentication for ElasticSearch instance", default=None)
    parser.add_argument("--full-index", dest="full_index", help="Indexes all recorded tours for the given user. Might use significantly more time, depending on the number of recoded tours. Otherwise only the latest 10 tours are indexed.", action="store_true")
    parser.add_argument("-c", "--cookie", dest="cookie", help="Cookie value of a valid session (used for authentication).", required=True)
    parser.add_argument("-l", "--log", dest="log_level", choices=['debug', 'info', 'warning', 'error', 'critical'], help="Set the logging level", default='error')
    args = parser.parse_args()

    main(args)
