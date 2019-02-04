from cassandra.query import BatchStatement


class PredictionsByCSVRepo:

    def __init__(self, session):
        self.session = session

        insert_statement = """
        INSERT INTO usi_csv_predictions_by_csv (
            csv_file_date,
            keyword,
            informational,
            navigational,
            transactional
        ) VALUES (?,?,?,?,?)"""

        self.prep_insert_statement = session.prepare(insert_statement)

    def insert(self, values):
        self.session.execute(self.prep_insert_statement, values)

    def batch_insert(self, batch_values):
        batch = BatchStatement()
        for val_set in batch_values:
            batch.add(self.prep_insert_statement, val_set)

        self.session.execute(batch)
