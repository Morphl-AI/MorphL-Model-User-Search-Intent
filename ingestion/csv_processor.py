from keywords_repo import KeywordsByCSVRepo
from session_manager import CassandraSessionManager

class CsvProcessor:

	def __init__(self):
		self.USI_CSV_GROUP_ID = "ID_GRUPO"
		self.USI_CSV_KEYWORD_ID = "ID_KEYWORD"
		self.USI_CSV_TIMESTAMP = "FECHA"
		self.USI_CSV_IMPRESSIONS = "IMPRESSIONS"
		self.USI_CSV_CLICKS = "CLICKS"
		self.USI_CSV_KEYWORD = "KEYWORD"

		self.session_manager = CassandraSessionManager()
		self.keywords_repo = KeywordsByCSVRepo(self.session_manager.session)

	def format_date(self, date_str):
		return "{}-{}-{}".format(date_str[:4], date_str[4:6], date_str[6:8])

	def process_df(self, partition_df, csv_date):
		def persist_row (series_obj, date):
			values = [date, str(series_obj[self.USI_CSV_GROUP_ID]), str(series_obj[self.USI_CSV_KEYWORD_ID]), self.format_date(str(series_obj[self.USI_CSV_TIMESTAMP])), series_obj[self.USI_CSV_IMPRESSIONS], series_obj[self.USI_CSV_CLICKS], series_obj[self.USI_CSV_KEYWORD]]
			self.keywords_repo.insert(values)

		partition_df.apply(persist_row, date=csv_date)
