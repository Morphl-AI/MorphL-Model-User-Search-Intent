import os

from predictions_repo import PredictionsRepo
from predictions_by_csv_repo import PredictionsByCSVRepo
from predictions_statistics_repo import PredictionsStatisticsRepo
from session_manager import CassandraSessionManager


class BatchInference:

    def __init__(self):
        self.session_manager = CassandraSessionManager()
        self.predictions_repo = PredictionsRepo(self.session_manager.session)
        self.predictions_by_csv_repo = PredictionsByCSVRepo(
            self.session_manager.session)
        self.predictions_statistics_repo = PredictionsStatisticsRepo(
            self.session_manager.session)

    def run(self):
        print('Run batch inference')


def main():
    batch_inference = BatchInference()
    batch_inference.run()


if __name__ == '__main__':
    main()
