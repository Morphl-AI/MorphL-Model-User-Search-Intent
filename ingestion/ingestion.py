from os import getenv, path, listdir

import gcsfs
from pyspark.sql import functions as f, SparkSession
from pyspark.sql.types import StringType
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')
CASS_REQ_TIMEOUT = getenv('CASS_REQ_TIMEOUT')

USI_GOOGLE_CLOUD_PROJECT = getenv('USI_GOOGLE_CLOUD_PROJECT')
USI_GOOGLE_CLOUD_BUCKET = getenv('USI_GOOGLE_CLOUD_BUCKET')
USI_GOOGLE_CLOUD_PROCESSED = getenv('USI_GOOGLE_CLOUD_PROCESSED')
USI_GOOGLE_CLOUD_UNPROCESSED = getenv(
    'USI_GOOGLE_CLOUD_UNPROCESSED')
USI_GOOGLE_CLOUD_SERVICE_ACCOUNT = getenv(
    'USI_GOOGLE_CLOUD_SERVICE_ACCOUNT')


MASTER_URL = 'local[*]'
APPLICATION_NAME = 'ingest_csv'
USI_LOCAL_PATH = getenv('USI_LOCAL_PATH')

USI_CSV_GROUP_ID = "ID_GRUPO"
USI_CSV_KEYWORD_ID = "ID_KEYWORD"
USI_CSV_TIMESTAMP = "FECHA"
USI_CSV_IMPRESSIONS = "IMPRESSIONS"
USI_CSV_CLICKS = "CLICKS"
USI_CSV_KEYWORD = "KEYWORD"


def format_date(date_str):
    return "{}-{}-{}".format(date_str[:4], date_str[4:6], date_str[6:8])


def get_csv_date(csv_path):
    filename_without_extension = path.splitext(path.basename(csv_path))[0]
    return format_date(filename_without_extension)


def main():
    spark_session = (
        SparkSession.builder
        .appName(APPLICATION_NAME)
        .master(MASTER_URL)
        .config('spark.cassandra.connection.host', MORPHL_SERVER_IP_ADDRESS)
        .config('spark.cassandra.auth.username', MORPHL_CASSANDRA_USERNAME)
        .config('spark.cassandra.auth.password', MORPHL_CASSANDRA_PASSWORD)
        .config('spark.sql.shuffle.partitions', 16)
        .getOrCreate())

    log4j = spark_session.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    save_options_usi_csv_features_raw_p_df = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': 'usi_csv_features_raw_p'
    }

    fs = gcsfs.GCSFileSystem(
        project=USI_GOOGLE_CLOUD_PROJECT, token=USI_GOOGLE_CLOUD_SERVICE_ACCOUNT)

    auth_provider = PlainTextAuthProvider(
        username=MORPHL_CASSANDRA_USERNAME,
        password=MORPHL_CASSANDRA_PASSWORD
    )

    cluster = Cluster(
        [MORPHL_SERVER_IP_ADDRESS], auth_provider=auth_provider)

    spark_session_cass = cluster.connect(MORPHL_CASSANDRA_KEYSPACE)

    prep_stmt_predictions_statistics = spark_session_cass.prepare(
        'INSERT INTO usi_csv_files (always_zero, day_of_data_capture, is_processed) VALUES (0, ?, false)'
    )

    csv_files_local = listdir(USI_LOCAL_PATH)

    format_date_udf = f.udf(format_date, StringType())

    for csv_file in csv_files_local:
        print('Ingesting ' + csv_file + "...")

        df = spark_session.read.csv(
            USI_LOCAL_PATH + csv_file, sep="\t", header=True)

        csv_date = get_csv_date(csv_file)

        spark_session_cass.execute(
            prep_stmt_predictions_statistics, [csv_date], timeout=CASS_REQ_TIMEOUT)

        (df
         .withColumn('csv_file_date', f.lit(csv_date))
         .withColumn('timestamp', format_date_udf(USI_CSV_TIMESTAMP))
         .drop(USI_CSV_TIMESTAMP)
         .withColumnRenamed(USI_CSV_GROUP_ID, 'group_id')
         .withColumnRenamed(USI_CSV_KEYWORD_ID, 'keyword_id')
         .withColumnRenamed(USI_CSV_IMPRESSIONS, 'impressions')
         .withColumnRenamed(USI_CSV_CLICKS, 'clicks')
         .withColumnRenamed(USI_CSV_KEYWORD, 'keyword')
         .write
         .format('org.apache.spark.sql.cassandra')
         .mode('append')
         .options(**save_options_usi_csv_features_raw_p_df)
         .save()
         )

        path_from = USI_GOOGLE_CLOUD_BUCKET + '/' + \
            USI_GOOGLE_CLOUD_UNPROCESSED + '/' + csv_file
        path_to = USI_GOOGLE_CLOUD_BUCKET + '/' + \
            USI_GOOGLE_CLOUD_PROCESSED + "/" + \
            csv_file

        fs.mv(path_from, path_to)

        print('Done with ' + csv_file)

    cluster.shutdown()


if __name__ == '__main__':
    main()
