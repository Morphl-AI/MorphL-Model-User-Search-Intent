from os import getenv


class SparkSessionManager:

    def __init__(self):

        MASTER_URL = 'local[*]'
        APPLICATION_NAME = 'preprocessor'
        MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
        MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
        MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')

        self.spark_session = (
            SparkSession.builder
            .appName(APPLICATION_NAME)
            .master(MASTER_URL)
            .config('spark.cassandra.connection.host', MORPHL_SERVER_IP_ADDRESS)
            .config('spark.cassandra.auth.username', MORPHL_CASSANDRA_USERNAME)
            .config('spark.cassandra.auth.password', MORPHL_CASSANDRA_PASSWORD)
            .config('spark.sql.shuffle.partitions', 16)
            .config('parquet.enable.summary-metadata', 'true')
            .getOrCreate())

    def get_spark_df(self, load_options):
        return self.spark_session.read.format('org.apache.spark.sql.cassandra').options(**load_options).load()
