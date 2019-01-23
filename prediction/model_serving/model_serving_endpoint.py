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
            'predictions': {}
        }

        template_for_single_row = 'SELECT * FROM usi_csv_predictions WHERE keyword = ? LIMIT 1'

        self.prep_stmts['predictions']['single'] = self.session.prepare(
            template_for_single_row)

    def retrieve_prediction(self, keyword):
        bind_list = [keyword]
        return self.session.execute(self.prep_stmts['predictions']['single'], bind_list, timeout=self.CASS_REQ_TIMEOUT)._current_rows


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


@app.route('/search-intent/getprediction/<keyword>', methods=['GET'])
def get_prediction(keyword):
    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    # Escape input (special characters) and search database
    p = app.config['CASSANDRA'].retrieve_prediction(escape(keyword))

    if len(p) == 0:
        return jsonify(status=0, error='No associated predictions found for this keyword.')

    return jsonify(
        status=1,
        informational=p[0]['informational'],
        navigational=p[0]['navigational'],
        transactional=p[0]['transactional'],
    )


@app.route('/search-intent/getpredictions', methods=['POST'])
def get_predictions():
    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    keywords = request.form.getlist("keywords")

    results = []
    for keyword in keywords:
        p = app.config['CASSANDRA'].retrieve_prediction(escape(keyword))
        if len(p) == 0:
            results.append({
                'keyword': keyword,
                'error': 'No associated predictions found for this keyword.'
            })
        else:
            results.append({
                'keyword': keyword,
                'informational': p[0]['informational'],
                'navigational': p[0]['navigational'],
                'transactional': p[0]['transactional'],
            })

    return jsonify(status=1, predictions=results)


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
