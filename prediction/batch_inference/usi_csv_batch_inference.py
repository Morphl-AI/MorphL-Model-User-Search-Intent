from os import getenv
import numpy as np

from pyspark.sql import functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType
from pyspark.sql.types import FloatType

from csv_files_repo import CSVFilesRepo

from spark_session_manager import SparkSessionManager
from predictions_by_csv_repo import PredictionsByCSVRepo
from predictions_statistics_repo import PredictionsStatisticsRepo
from session_manager import CassandraSessionManager
from model_manager import ModelManager
from embedding_manager import EmbeddingManager


class BatchInference:

    def __init__(self):

        self.MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

        self.session_manager = CassandraSessionManager()
        self.spark_session_manager = SparkSessionManager()
        self.model_manager = ModelManager()
        self.embedding_manager = EmbeddingManager()

        self.csv_files_repo = CSVFilesRepo(self.session_manager.session)
        self.predictions_by_csv_repo = PredictionsByCSVRepo(
            self.session_manager.session)
        self.predictions_statistics_repo = PredictionsStatisticsRepo(
            self.session_manager.session)

    def save_predictions(self, df):
        if len(df.head(1)) > 0:
            save_options_usi_predictions_by_csv_file = {
                'keyspace': self.MORPHL_CASSANDRA_KEYSPACE,
                'table': 'usi_csv_predictions_by_csv'}

            (df.repartition(32).write
             .format('org.apache.spark.sql.cassandra')
                .mode('append')
               .options(**save_options_usi_predictions_by_csv_file)
             .save())

            save_options_usi_predictions = {
                'keyspace': self.MORPHL_CASSANDRA_KEYSPACE,
                'table': 'usi_csv_predictions'}

            (df.drop('csv_file_date').repartition(32)
             .write
             .format('org.apache.spark.sql.cassandra')
                .mode('append')
               .options(**save_options_usi_predictions)
             .save())

            def count_prediction(condition): return f.sum(
                f.when(condition, 1).otherwise(0))

            statistics_df = df.groupBy('csv_file_date').agg(
                count_prediction(f.col('informational') >
                                 0.5).alias('informational'),
                count_prediction(f.col('transactional') >
                                 0.5).alias('transactional'),
                count_prediction(f.col('navigational') >
                                 0.5).alias('navigational')
            )

            for intent in ['informational', 'navigational', 'transactional']:
                no_predictions = statistics_df.collect()[0][intent]

                self.predictions_statistics_repo.update(
                    intent, [no_predictions])

    def run(self):
        print('Run batch inference')

        # Select unprocessed CSV files
        csv_files = self.csv_files_repo.select([False])._current_rows

        if len(csv_files) == 0:
            return

        load_options = {
            'keyspace': self.MORPHL_CASSANDRA_KEYSPACE,
            'table': 'usi_csv_features_raw_p',
            'spark.cassandra.input.fetch.size_in_rows': '150'}

        raw_features_df = (self.spark_session_manager.get_spark_df(
            load_options))

        def process_keyword(keyword):
            try:
                embedding_manager = EmbeddingManager()
                word_vector = embedding_manager.get_words_embeddings(keyword)
            except Exception as e:
                word_vector = None

            if word_vector is not None:
                model_manager = ModelManager()
                prediction = model_manager.predict(word_vector)
                predictions_list = prediction.tolist()[0]
                return predictions_list

        processor_udf = udf(process_keyword, ArrayType(FloatType()))

        for csv_file in csv_files:
            print('Processing ', csv_file['day_of_data_capture'])

            features_by_date_df = raw_features_df.filter(
                "csv_file_date == '{}'".format(csv_file['day_of_data_capture']))

            predictions_df = features_by_date_df.select(processor_udf("keyword").alias(
                "predictions"), features_by_date_df['keyword'], features_by_date_df['csv_file_date'])

            predictions_df = predictions_df.filter(f.col("predictions").isNotNull()).select(predictions_df['csv_file_date'], predictions_df['keyword'], predictions_df.predictions[0].alias(
                'informational'), predictions_df.predictions[1].alias('navigational'), predictions_df.predictions[2].alias('transactional'))

            self.save_predictions(
                predictions_df)

            self.csv_files_repo.update([True, csv_file['day_of_data_capture']])


def main():
    batch_inference = BatchInference()
    batch_inference.run()


if __name__ == '__main__':
    main()
