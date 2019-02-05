from os import getenv

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import dict_factory


class CassandraSessionManager:

    def __init__(self):
        self.MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
        self.MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
        self.MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
        self.MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

        self.auth_provider = PlainTextAuthProvider(
            username=self.MORPHL_CASSANDRA_USERNAME, password=self.MORPHL_CASSANDRA_PASSWORD)
        self.cluster = Cluster(contact_points=[
                               self.MORPHL_SERVER_IP_ADDRESS], auth_provider=self.auth_provider)

        self.session = self.cluster.connect(self.MORPHL_CASSANDRA_KEYSPACE)
        self.session.row_factory = dict_factory
        self.session.default_fetch_size = 100
