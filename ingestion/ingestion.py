import os

from csv_processor import CsvProcessor
from gs_manager import GoogleStorageManager
from keywords_repo import KeywordsByCSVRepo
from session_manager import CassandraSessionManager


class DataIngestion:

	def __init__(self):
		self.session_manager = CassandraSessionManager()
		self.keywords_repo = KeywordsByCSVRepo(self.session_manager.session)
		self.storage_manager = GoogleStorageManager()
		self.csv_processor = CsvProcessor()

	def row_processor(self, csv_path):
		filename_without_extension = os.path.splitext(os.path.basename(csv_path))[0]
		return lambda values: [values.insert(0, self.csv_processor.format_date(filename_without_extension)), self.keywords_repo.insert(values)]

	def run(self):
		for csv_path in self.storage_manager.get_unprocessed_filenames():
			print('Ingesting ' + csv_path + "...")

			df = self.storage_manager.get_df(csv_path)
			self.csv_processor.process_df(df, self.row_processor(csv_path))
			self.storage_manager.move_to_processed(csv_path)

			print('Done with ' + csv_path)

		self.session_manager.cluster.shutdown()


def main():
	data_ingestion = DataIngestion()
	data_ingestion.run()


if __name__ == '__main__':
	main()
