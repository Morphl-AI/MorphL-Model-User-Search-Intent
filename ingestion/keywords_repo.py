from cassandra.query import BatchStatement


class KeywordsByCSVRepo:

	def __init__(self, session):
		self.session = session

		insert_statement = """
		INSERT INTO usi_csv_features_raw_p (
		csv_file_date,
		group_id,
		keyword_id,
		timestamp,
		impressions,
		clicks,
		keyword
		)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		"""

		self.prep_insert_statement = session.prepare(insert_statement)

	def insert(self, values):
		self.session.execute(self.prep_insert_statement, values, timeout=3600.0)

	def batch_insert(self, batch_values):
		batch = BatchStatement()
		for val_set in batch_values:
			batch.add(self.prep_insert_statement, val_set)

		self.session.execute(batch, timeout=3600.0)
