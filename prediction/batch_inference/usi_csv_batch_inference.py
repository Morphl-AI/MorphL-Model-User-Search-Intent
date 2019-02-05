from os import getenv
import numpy as np

from predictions_features_raw_repo import PredictionsFeaturesRawRepo
from predictions_repo import PredictionsRepo
from predictions_by_csv_repo import PredictionsByCSVRepo
from predictions_statistics_repo import PredictionsStatisticsRepo
from session_manager import CassandraSessionManager
from model_manager import ModelManager
from embedding_manager import EmbeddingManager

DAY_AS_STR = getenv('DAY_AS_STR')


class BatchInference:

    def __init__(self):
        self.session_manager = CassandraSessionManager()
        self.model_manager = ModelManager()
        self.embedding_manager = EmbeddingManager()

        self.predictions_features_raw_repo = PredictionsFeaturesRawRepo(
            self.session_manager.session)
        self.predictions_repo = PredictionsRepo(self.session_manager.session)
        self.predictions_by_csv_repo = PredictionsByCSVRepo(
            self.session_manager.session)
        self.predictions_statistics_repo = PredictionsStatisticsRepo(
            self.session_manager.session)

    def save_predictions(self, values):
        if len(values) > 0:
            batch_values = [list(val_set.values()) for val_set in values]
            self.predictions_repo.batch_insert(batch_values)

    def run(self):
        print('Run batch inference')

        has_more_pages = True
        paging_state = None

        i = 0

        while has_more_pages:
            # Read raw keywords from the database
            results = self.predictions_features_raw_repo.select(
                DAY_AS_STR, paging_state)

            values = []

            for row in results._current_rows:
                # Get embeddings for each keyword
                try:
                    word_vec = self.embedding_manager.get_words_embeddings(
                        row['keyword'])
                except Exception as e:
                    word_vec = None
                    print(row['keyword'], "Some words not in dict", e)

                if word_vec is not None:
                    # Get prediction values
                    prediction = self.model_manager.predict(word_vec)

                    # !!! Order matters for the insert statements.
                    values.append({
                        'keyword': row['keyword'],
                        'informational': prediction[0][1],
                        'navigational': prediction[0][2],
                        'transactional': prediction[0][0]
                    })

            self.save_predictions(values)

            has_more_pages = results.has_more_pages
            paging_state = results.paging_state

            i += 1

            if i > 2:
                break


def main():
    batch_inference = BatchInference()
    batch_inference.run()


if __name__ == '__main__':
    main()
