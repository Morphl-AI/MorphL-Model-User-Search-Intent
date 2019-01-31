class CsvProcessor:

	def __init__(self):
		self.USI_CSV_GROUP_ID = "ID_GRUPO"
		self.USI_CSV_KEYWORD_ID = "ID_KEYWORD"
		self.USI_CSV_TIMESTAMP = "FECHA"
		self.USI_CSV_IMPRESSIONS = "IMPRESSIONS"
		self.USI_CSV_CLICKS = "CLICKS"
		self.USI_CSV_KEYWORD = "KEYWORD"

	def format_date(self, date_str):
		return "{}-{}-{}".format(date_str[:4], date_str[4:6], date_str[6:8])

	def get_row_values(self, row):
		return [str(row[self.USI_CSV_GROUP_ID]), str(row[self.USI_CSV_KEYWORD_ID]), self.format_date(str(row[self.USI_CSV_TIMESTAMP])), row[self.USI_CSV_IMPRESSIONS], row[self.USI_CSV_CLICKS], row[self.USI_CSV_KEYWORD]]

	def process_df(self, df, row_processor):
		for index, row in df.iterrows():
			values = self.get_row_values(row)
			row_processor(values)
