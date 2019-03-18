cp -r /opt/usi_csv /opt/code
cd /opt/code
git pull
spark-submit --jars /opt/spark/jars/spark-cassandra-connector.jar,/opt/spark/jars/jsr166e.jar  /opt/code/prediction/preprocessing/usi_csv_preprocessing.py
