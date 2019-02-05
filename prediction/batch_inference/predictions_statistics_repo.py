

class PredictionsStatisticsRepo:

    def __init__(self, session):
        self.session = session

        self.prep_update_statement = {
            'informational': session.prepare(
                'UPDATE usi_csv_predictions_statistics SET informational = informational + ? WHERE always_zero = 0'
            ),
            'navigational': session.prepare(
                'UPDATE usi_csv_predictions_statistics SET navigational = navigational + ? WHERE always_zero = 0'
            ),
            'transactional': session.prepare(
                'UPDATE usi_csv_predictions_statistics SET transactional = transactional + ? WHERE always_zero = 0'
            )
        }

    def update(self, counter, values):
        self.session.execute(
            self.prep_update_statement[counter], values, timeout=3600.0)
