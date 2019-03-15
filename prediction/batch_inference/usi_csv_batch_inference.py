import datetime
from os import getenv
from pyspark.sql import functions as f, SparkSession
import torchtext.vocab as vocab
import torch as tr
from pyspark.sql.types import ArrayType
from pyspark.sql.types import FloatType
from keras.models import load_model, model_from_json
import keras.backend as K
import numpy as np

MASTER_URL = 'local[*]'
APPLICATION_NAME = 'preprocessor'
MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')
MODELS_DIR = '/opt/models'

model_file = f'{MODELS_DIR}/usi_csv_en_model.h5'


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


model = load_model(model_file, custom_objects={'kerasAcc': model_accuracy})

model_json_string = model.to_json()
model_weights = model.get_weights()

model = model_from_json(model_json_string)
model.set_weights(model_weights)


def predict_intent(embeddings):
    return model.predict(tr.tensor([embeddings]).numpy()).tolist()[0]


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

    save_options_usi_predictions = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': 'usi_csv_predictions'
    }

    processed_features_df = (fetch_from_cassandra(
        'usi_csv_word_embeddings', spark_session))

    processed_features_df.show(1)

    predict_udf = f.udf(predict_intent, ArrayType(FloatType()))

    predictions_df = processed_features_df.select(
        'keyword', predict_udf("embeddings").alias("predictions"))

    predictions_df_final = predictions_df.select(
        'keyword',
        predictions_df.predictions[0].alias('informational'),
        predictions_df.predictions[1].alias('navigational'),
        predictions_df.predictions[2].alias('transactional'),
    ).repartition(32)

    (predictions_df_final
     .write
     .format('org.apache.spark.sql.cassandra')
     .mode('append')
     .options(**save_options_usi_predictions)
     .save()
     )
