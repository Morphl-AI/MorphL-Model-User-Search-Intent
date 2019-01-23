CREATE KEYSPACE IF NOT EXISTS morphl WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE morphl.usi_csv_predictions (
  keyword text,
  informational double,
  navigational double,
  transactional double,
  PRIMARY KEY (keyword)
);

CREATE TABLE morphl.usi_csv_predictions_by_date (
  keyword text,
  day_of_data_capture date,
  informational double,
  navigational double,
  transactional double,
  PRIMARY KEY (keyword, day_of_data_capture)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);
