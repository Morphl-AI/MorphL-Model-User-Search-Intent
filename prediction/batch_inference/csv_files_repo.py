

class CSVFilesRepo:

    def __init__(self, session):
        self.session = session

        select_statement = "SELECT * from usi_csv_files WHERE always_zero = 0 AND is_processed = ? ALLOW FILTERING"
        self.prep_select_statement = session.prepare(select_statement)

        update_statement = "UPDATE usi_csv_files SET processed = ? WHERE always_zero = 0 AND day_of_data_capture = ?"
        self.prep_update_statement = session.prepare(update_statement)

    def select(self, values):
        return self.session.execute(self.prep_select_statement, values, timeout=3600.0)

    def update(self, values):
        self.session.execute(self.prep_update_statement,
                             values, timeout=3600.0)
