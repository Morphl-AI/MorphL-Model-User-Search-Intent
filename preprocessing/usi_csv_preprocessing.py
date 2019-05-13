import datetime
from os import getenv
from pyspark.sql import functions as f, SparkSession
import torchtext.vocab as vocab
import torch.tensor as tensor
from pyspark.sql.types import ArrayType
from pyspark.sql.types import DoubleType


# Load env varibales
MASTER_URL = 'local[*]'
APPLICATION_NAME = 'preprocessor'
MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

# Load word embeddings tensor
embedding = vocab.GloVe(name="6B", dim=100)

# Function that returns a dataframe from a cassandra table


def fetch_from_cassandra(c_table_name, spark_session):
    load_options = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': c_table_name,
        'spark.cassandra.input.fetch.size_in_rows': '150'}

    df = (spark_session.read.format('org.apache.spark.sql.cassandra')
                            .options(**load_options)
                            .load())

    return df


# Function that fetches word embedding from embeddings tensor , will be used as udf later
def process_keyword(keywords):
    result = []

    try:
        for keyword in keywords:
            index = embedding.stoi[keyword]
            result.append(embedding.vectors[index].tolist())
    except Exception as e:
        return

    return result


def main():
    # Init spark session
    spark_session = (
        SparkSession.builder
        .appName(APPLICATION_NAME)
        .master(MASTER_URL)
        .config('spark.cassandra.connection.host', MORPHL_SERVER_IP_ADDRESS)
        .config('spark.cassandra.auth.username', MORPHL_CASSANDRA_USERNAME)
        .config('spark.cassandra.auth.password', MORPHL_CASSANDRA_PASSWORD)
        .config('spark.sql.shuffle.partitions', 16)
        .config('parquet.enable.summary-metadata', 'true')
        .getOrCreate())

    # Save options for embeddings table
    save_options_usi_csv_word_embeddings = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': 'usi_csv_word_embeddings'}

    # Save options for the csv_files table
    save_options_usi_csv_files = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': 'usi_csv_files'
    }

    # Register process_keyword as a udf that returns an array of arrays filled with doubles
    processor_udf = f.udf(process_keyword, ArrayType(ArrayType(DoubleType())))

    # Fetch a dataframe from the usi_csv_files table and filter for unprocessed files
    usi_csv_files_df = (fetch_from_cassandra('usi_csv_files', spark_session)
                        .filter('always_zero = 0 AND is_processed = False'))

    # Iterate through the csv_files dataframe one by one, this should not be a problem since
    # the number of csv files will rarely get higher than the 1000s
    for csv_file in usi_csv_files_df.collect():
        print('Processing ', csv_file['day_of_data_capture'])

        # Fetch a dataframe from the usi_csv_raw_p table,
        # add a new column with an array of sanitzedkeywords extracted
        # from the string from the 'keyword' column
        # and filter them by the current csv_file date
        usi_csv_features_raw_p_df = (
            fetch_from_cassandra('usi_csv_features_raw_p', spark_session)
            .withColumn('clean_keywords', f.split(f.lower(f.regexp_replace('keyword', '\\+', '')), ' '))
            .filter("csv_file_date == '{}'".format(csv_file['day_of_data_capture'])))

        # Create a new dataframe with the csv_file_date and keyword columns
        # and apply an udf to the "clean_keywords" column to get their embeddings
        # then drop all the cases where null was saved because the embeddings vector
        # did not contain a word
        usi_csv_word_embeddings = (usi_csv_features_raw_p_df
                                   .select(
                                       'csv_file_date',
                                       'keyword',
                                       processor_udf("clean_keywords").alias("embeddings"))
                                   .na.drop(subset=["embeddings"])
                                   .repartition(32))

        # Write the new dataframe to the embeddings table
        (usi_csv_word_embeddings
         .write
         .format('org.apache.spark.sql.cassandra')
         .mode('append')
         .options(**save_options_usi_csv_word_embeddings)
         .save())

    # Mark all the processed csv files as such in the database
    (usi_csv_files_df
        .withColumn('is_processed', f.lit(True))
        .write
        .format('org.apache.spark.sql.cassandra')
        .mode('append')
        .options(**save_options_usi_csv_files)
        .save())


if __name__ == '__main__':
    main()
