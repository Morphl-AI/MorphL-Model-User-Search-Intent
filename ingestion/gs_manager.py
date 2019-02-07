import os
from os import getenv

import gcsfs
import pandas as pd


class GoogleStorageManager:

	def __init__(self):
		self.USI_GOOGLE_CLOUD_PROJECT = getenv('USI_GOOGLE_CLOUD_PROJECT')
		self.USI_GOOGLE_CLOUD_BUCKET = getenv('USI_GOOGLE_CLOUD_BUCKET')
		self.USI_GOOGLE_CLOUD_PROCESSED = getenv('USI_GOOGLE_CLOUD_PROCESSED')
		self.USI_GOOGLE_CLOUD_UNPROCESSED = getenv('USI_GOOGLE_CLOUD_UNPROCESSED')
		self.USI_GOOGLE_CLOUD_SERVICE_ACCOUNT = getenv('USI_GOOGLE_CLOUD_SERVICE_ACCOUNT')

		self.fs = gcsfs.GCSFileSystem(project=self.USI_GOOGLE_CLOUD_PROJECT, token=self.USI_GOOGLE_CLOUD_SERVICE_ACCOUNT)

	def get_filenames(self, path_in_bucket=''):
		csv_full_paths = self.fs.ls(self.USI_GOOGLE_CLOUD_BUCKET + '/' + path_in_bucket)
		csv_paths_in_bucket = []
		for csv_full_path in csv_full_paths:
			csv_paths_in_bucket.append(csv_full_path.replace(self.USI_GOOGLE_CLOUD_BUCKET + '/', '', 1))

		return csv_paths_in_bucket

	def get_unprocessed_filenames(self):
		return self.get_filenames(self.USI_GOOGLE_CLOUD_UNPROCESSED)

	def move_to_processed(self, path_in_bucket):
		path_from = self.USI_GOOGLE_CLOUD_BUCKET + '/' + path_in_bucket
		path_to = self.USI_GOOGLE_CLOUD_BUCKET + '/' + self.USI_GOOGLE_CLOUD_PROCESSED + "/" + os.path.basename(path_in_bucket)
		self.fs.mv(path_from, path_to)

	def get_df(self, csv_path_in_bucket):
		with self.fs.open(self.USI_GOOGLE_CLOUD_BUCKET + '/' + csv_path_in_bucket) as f:
			df = pd.read_csv(f, sep="\t")
			return df
