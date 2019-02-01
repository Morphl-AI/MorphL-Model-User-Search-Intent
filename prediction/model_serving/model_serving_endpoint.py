from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory
from cassandra.protocol import ProtocolException

from operator import itemgetter

from flask import (render_template as rt,
                   Flask, request, redirect, url_for, session, jsonify, escape)
from flask_cors import CORS

from gevent.pywsgi import WSGIServer

import jwt
import re
from datetime import datetime, timedelta

"""
    Database connector
"""


class Cassandra:
    def __init__(self):
        self.MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
        self.MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
        self.MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
        self.MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

        self.QUERY = 'SELECT * FROM usi_csv_predictions WHERE keyword = ? LIMIT 1'
        self.CASS_REQ_TIMEOUT = 3600.0

        self.auth_provider = PlainTextAuthProvider(
            username=self.MORPHL_CASSANDRA_USERNAME,
            password=self.MORPHL_CASSANDRA_PASSWORD)
        self.cluster = Cluster(
            [self.MORPHL_SERVER_IP_ADDRESS], auth_provider=self.auth_provider)

        self.session = self.cluster.connect(self.MORPHL_CASSANDRA_KEYSPACE)
        self.session.row_factory = dict_factory
        self.session.default_fetch_size = 100

        self.prepare_statements()

    def prepare_statements(self):
        """
            Prepare statements for database select queries
        """
        self.prep_stmts = {
            'csvs': {},
            'predictions': {},
            'statistics': {}
        }

        # Find last 100 csv files
        self.prep_stmts['csvs']['multiple'] = self.session.prepare(
            'SELECT day_of_data_capture FROM usi_csv_files WHERE always_zero = 0 LIMIT 100')

        # Find a prediction by keyword
        self.prep_stmts['predictions']['single'] = self.session.prepare(
            'SELECT * FROM usi_csv_predictions WHERE keyword = ? LIMIT 1')

        # Find predictions by csv file (date)
        self.prep_stmts['predictions']['multiple_by_csv'] = self.session.prepare(
            'SELECT keyword, informational, navigational, transactional FROM usi_csv_predictions_by_csv WHERE csv_file_date = ?')

        # Find predictions by csv file (date) and keyword
        self.prep_stmts['predictions']['multiple_by_csv_keyword'] = self.session.prepare(
            'SELECT keyword, informational, navigational, transactional FROM usi_csv_predictions_by_csv WHERE csv_file_date = ? AND keyword = ?')

        # Find predictions statistics (counters)
        self.prep_stmts['statistics']['count'] = self.session.prepare(
            'SELECT informational, navigational, transactional FROM usi_csv_predictions_statistics WHERE always_zero = 0')

    def retrieve_csvs(self):
        return self.session.execute(self.prep_stmts['csvs']['multiple'], timeout=self.CASS_REQ_TIMEOUT)._current_rows

    def retrieve_prediction(self, keyword):
        bind_list = [keyword]
        return self.session.execute(self.prep_stmts['predictions']['single'], bind_list, timeout=self.CASS_REQ_TIMEOUT)._current_rows

    def retrieve_predictions_by_csv(self, csv_file_date, paging_state=None):
        # Documentation: https://datastax.github.io/python-driver/query_paging.html#resume-paged-results

        bind_list = [csv_file_date]
        if paging_state is not None:
            try:
                previous_paging_state = bytes.fromhex(paging_state)
                results = self.session.execute(
                    self.prep_stmts['predictions']['multiple_by_csv'], bind_list, paging_state=previous_paging_state, timeout=self.CASS_REQ_TIMEOUT)
            except (ValueError, ProtocolException):
                return {'status': 0, 'error': 'Invalid pagination request.'}

        else:
            results = self.session.execute(
                self.prep_stmts['predictions']['multiple_by_csv'], bind_list, timeout=self.CASS_REQ_TIMEOUT)

        return {
            'predictions': results._current_rows,
            'next_paging_state': results.paging_state.hex(
            ) if results.has_more_pages == True else 0
        }

    def retrieve_predictions_by_csv_keyword(self, csv_file_date, keyword):
        bind_list = [csv_file_date, keyword]
        return self.session.execute(self.prep_stmts['predictions']['multiple_by_csv_keyword'], bind_list, timeout=self.CASS_REQ_TIMEOUT)._current_rows

    def retrieve_statistics(self):
        return self.session.execute(self.prep_stmts['statistics']['count'], timeout=self.CASS_REQ_TIMEOUT)._current_rows


"""
    API class for verifying credentials and handling JWTs.
"""


class API:
    def __init__(self):
        self.API_DOMAIN = getenv('API_DOMAIN')
        self.MORPHL_API_KEY = getenv('MORPHL_API_KEY')
        self.MORPHL_API_JWT_SECRET = getenv('MORPHL_API_JWT_SECRET')

    def verify_jwt(self, token):
        try:
            decoded = jwt.decode(token, self.MORPHL_API_JWT_SECRET)
        except Exception:
            return False

        return (decoded['iss'] == self.API_DOMAIN and
                decoded['sub'] == self.MORPHL_API_KEY)


app = Flask(__name__)
CORS(app)

# @todo Check request origin for all API requests


@app.route("/search-intent")
def main():
    return "MorphL Predictions API - User Search Intent"

# Find csv files


@app.route('/search-intent/csvs', methods=['GET'])
def get_csvs():
    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    csvs = app.config['CASSANDRA'].retrieve_csvs()

    results = []
    for csv in csvs:
        results.append(csv['day_of_data_capture'].date().strftime('%Y-%m-%d'))

    return jsonify(
        status=1,
        csvs=results
    )

# Find a single prediction by keyword


@app.route('/search-intent/predictions/<keyword>', methods=['GET'])
def get_prediction(keyword):
    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    # Escape input (special characters) and search database
    p = app.config['CASSANDRA'].retrieve_prediction(escape(keyword.lower()))

    if len(p) == 0:
        return jsonify(status=0, error='No associated predictions found for this keyword.')

    return jsonify(
        status=1,
        informational=p[0]['informational'],
        navigational=p[0]['navigational'],
        transactional=p[0]['transactional'],
    )

# Find multiple predictions by keyword


@app.route('/search-intent/predictions', methods=['POST'])
def get_predictions():
    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    keywords = request.form.getlist("keywords")

    results = []
    for keyword in keywords:
        p = app.config['CASSANDRA'].retrieve_prediction(
            escape(keyword.lower()))
        if len(p) == 0:
            results.append({
                'keyword': keyword,
                'error': 'No associated predictions found for this keyword.'
            })
        else:
            results.append(p[0])

    return jsonify(status=1, predictions=results)

# Find multiple predictions by keyword


@app.route('/search-intent/csvs/<csv_file_date>/predictions', methods=['GET'], defaults={'keyword': None})
@app.route('/search-intent/csvs/<csv_file_date>/predictions/<keyword>', methods=['GET'])
def get_predictions_by_csv(csv_file_date, keyword):
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    # Validate date
    try:
        csv_file_date = datetime.strptime(csv_file_date, '%Y-%m-%d')
    except ValueError:
        return jsonify(status=0, error='Invalid date format.'), 401

    # Validate page
    if not request.args.get('page') is None and not re.match('^[a-zA-Z0-9_]+$', request.args.get('page')):
        return jsonify(status=0, error='Invalid page format.')

    # Read predictions from the database
    if keyword is not None:
        # Escape input (special characters) and search database for particular keyword
        p = app.config['CASSANDRA'].retrieve_predictions_by_csv_keyword(
            csv_file_date, escape(keyword.lower()))

        return jsonify(
            status=1,
            predictions=p,
            next_paging_state=0
        )

    results = app.config['CASSANDRA'].retrieve_predictions_by_csv(
        csv_file_date,
        request.args.get('page')
    )

    return jsonify(
        status=1,
        predictions=results['predictions'],
        next_paging_state=results['next_paging_state']
    )

# Find prediction statistics (counter)


@app.route('/search-intent/statistics', methods=['GET'])
def get_statistics():
    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    s = app.config['CASSANDRA'].retrieve_statistics()

    if len(s) == 0:
        return jsonify(status=0, error='No associated statistics found.')

    return jsonify(
        status=1,
        informational=int(s[0]['informational'] or 0),
        navigational=int(s[0]['navigational'] or 0),
        transactional=int(s[0]['transactional'] or 0),
    )


if __name__ == '__main__':
    app.config['CASSANDRA'] = Cassandra()
    app.config['API'] = API()
    if getenv('DEBUG'):
        app.config['DEBUG'] = True
        flask_port = 5858
        app.run(host='0.0.0.0', port=flask_port)
    else:
        app.config['DEBUG'] = False
        flask_port = 6868
        WSGIServer(('', flask_port), app).serve_forever()
