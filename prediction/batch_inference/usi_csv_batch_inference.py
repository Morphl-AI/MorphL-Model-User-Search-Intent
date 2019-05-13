import datetime
from os import getenv
from pyspark.sql import functions as f, SparkSession
import torchtext.vocab as vocab
import torch as tr
from pyspark.sql.types import ArrayType
from pyspark.sql.types import DoubleType
from keras.models import load_model, model_from_json
import keras.backend as K
import numpy as np


# Get env variables
MASTER_URL = 'local[*]'
APPLICATION_NAME = 'preprocessor'
MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')
MODELS_DIR = getenv('MODELS_DIR')

# Get model path
model_file = f'{MODELS_DIR}/usi_csv_en_model.h5'

# Custom accuracy function for the prediction model,
# only needed for training, but the model won't load without it


def model_accuracy(y_true, y_pred):
    std = K.std(y_true, axis=1)
    where1Class = K.cast(K.abs(std - 0.4714) < 0.01, "float32")
    where2Class = K.cast(K.abs(std - 0.2357) < 0.01, "float32")
    where3Class = K.cast(K.abs(std) < 0.01, "float32")
    thresholds = where1Class * \
        K.constant(0.5) + where2Class * K.constant(0.25) + \
        where3Class * K.constant(0.16)
    thresholds = K.expand_dims(thresholds, axis=-1)

    y_pred = K.cast(y_pred > thresholds, "int32")
    y_true = K.cast(y_true > 0, "int32")
    res = y_pred + y_true
    res = K.cast(K.equal(res, 2 * K.ones_like(res)), "int32")

    res = K.cast(K.sum(res, axis=1) > 0, "float32")
    res = K.mean(res)

    return res


# Load model with keras, using the custom accuracy function and model file path
model = load_model(model_file, custom_objects={'kerasAcc': model_accuracy})

# Get model json string
model_json_string = model.to_json()
# Get model weights
model_weights = model.get_weights()

# Load model from json string and set weights so that it can be
# serialized correctly when being called inside the UDF
model = model_from_json(model_json_string)
model.set_weights(model_weights)


# Define UDF function that predicts users intent using the model,
# based on word embeddings vector
def predict_intent(embeddings):
    # Transform embeddings lists to tensor then numpy array and get predictions as list
    return model.predict(tr.tensor([embeddings]).numpy()).tolist()[0]

# Define aggregation function that checks the given condition


def count_prediction(condition): return f.sum(
    f.when(condition, 1).otherwise(0))


# Cassandra read connector function
def fetch_from_cassandra(c_table_name, spark_session):
    load_options = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': c_table_name,
        'spark.cassandra.input.fetch.size_in_rows': '150'}

    df = (spark_session.read.format('org.apache.spark.sql.cassandra')
                            .options(**load_options)
                            .load())
    return df


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

    # Get word embeddings from from cassandra
    embeddings_df = (fetch_from_cassandra(
        'usi_csv_word_embeddings', spark_session))

    # Register prediction UDF
    predict_udf = f.udf(predict_intent, ArrayType(DoubleType()))

    # Apply UDF to embeddings dataframe
    predictions_df = embeddings_df.select(
        'csv_file_date', 'keyword', predict_udf("embeddings").alias("predictions"))

    # Split predictions array based on category and save to cassandra
    predictions_by_csv_df = predictions_df.select(
        'csv_file_date',
        'keyword',
        predictions_df.predictions[0].alias('informational'),
        predictions_df.predictions[1].alias('navigational'),
        predictions_df.predictions[2].alias('transactional'),
    ).repartition(32)

    predictions_by_csv_df.cache()

    predictions_by_csv_df.createOrReplaceTempView('predictions_by_csv')

    save_options_usi_predictions_by_csv = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': 'usi_csv_predictions_by_csv'
    }

    (predictions_by_csv_df
     .write
     .format('org.apache.spark.sql.cassandra')
     .mode('append')
     .options(**save_options_usi_predictions_by_csv)
     .save()
     )

    # Save predictions without date to cassandra
    predictions_by_keyword_df = predictions_by_csv_df.drop(
        'csv_file_date').repartition(32)

    predictions_by_keyword_df.cache()

    predictions_by_keyword_df.createOrReplaceTempView('predictions')

    save_options_usi_predictions = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': 'usi_csv_predictions'
    }

    (predictions_by_keyword_df
     .write
     .format('org.apache.spark.sql.cassandra')
     .mode('append')
     .options(**save_options_usi_predictions)
     .save()
     )

    # Calculate predictions statistics and save to casssandra
    predictions_statistics_df = predictions_by_csv_df.groupBy('csv_file_date').agg(
        count_prediction(f.col('informational') >
                         0.5).alias('informational'),
        count_prediction(f.col('transactional') >
                         0.5).alias('transactional'),
        count_prediction(f.col('navigational') >
                         0.5).alias('navigational')
    ).repartition(32)

    predictions_statistics_df.cache()

    predictions_statistics_df.createOrReplaceTempView('predictions_statistics')

    save_options_usi_predictions_statistics = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': 'usi_csv_predictions_statistics'
    }

    (predictions_statistics_df
     .write
     .format('org.apache.spark.sql.cassandra')
     .mode('append')
     .options(**save_options_usi_predictions_statistics)
     .save()
     )


if __name__ == '__main__':
    main()
