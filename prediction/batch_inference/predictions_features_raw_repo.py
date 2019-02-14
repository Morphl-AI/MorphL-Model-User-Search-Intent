

class PredictionsFeaturesRawRepo:

    def __init__(self, session):
        self.session = session

        select_statement = "SELECT * FROM usi_csv_features_raw_p WHERE csv_file_date = ?"

        self.prep_select_statement = session.prepare(select_statement)

    def select(self, csv_file_date, paging_state=None):
        # Documentation: https://datastax.github.io/python-driver/query_paging.html#resume-paged-results
        bind_list = [csv_file_date]
        if paging_state is not None:
            return self.session.execute(
                self.prep_select_statement, bind_list, paging_state=paging_state, timeout=3600.0)

        return self.session.execute(self.prep_select_statement, bind_list, timeout=3600.0)
