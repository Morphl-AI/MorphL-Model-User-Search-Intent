CREATE KEYSPACE IF NOT EXISTS morphl WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE morphl.usi_csv_files (
  always_zero int,
  day_of_data_capture date,
  is_processed boolean,
  PRIMARY KEY ((always_zero), day_of_data_capture)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.usi_csv_features_raw_p (
  csv_file_date date,
  group_id text,
  keyword_id text,
  timestamp date,
  impressions int,
  clicks int,
  keyword text,
  PRIMARY KEY ((csv_file_date), keyword)
);

CREATE TABLE morphl.usi_csv_word_embeddings (
  csv_file_date date,
  keyword text,
  embeddings frozen<list<frozen<list<double>>>>,
  PRIMARY KEY ((csv_file_date, keyword))
);

CREATE TABLE morphl.usi_csv_predictions (
  keyword text,
  informational double,
  navigational double,
  transactional double,
  PRIMARY KEY (keyword)
);

CREATE TABLE morphl.usi_csv_predictions_by_csv (
  csv_file_date date,
  keyword text,
  informational double,
  navigational double,
  transactional double,
  PRIMARY KEY ((csv_file_date), keyword)
) WITH CLUSTERING ORDER BY (keyword ASC);

CREATE TABLE morphl.usi_csv_predictions_statistics (
  csv_file_date date,
  informational int,
  navigational int,
  transactional int,
  PRIMARY KEY (csv_file_date)
);