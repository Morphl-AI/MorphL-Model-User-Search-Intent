cp -r /opt/usi_csv /opt/code
cd /opt/code
git pull
spark-submit --jars /opt/spark/jars/spark-cassandra-connector.jar,/opt/spark/jars/jsr166e.jar --py-files /opt/code/prediction/batch_inference/embedding_manager.py,/opt/code/prediction/batch_inference/spark_session_manager.py,/opt/code/prediction/batch_inference/predictions_statistics_repo.py,/opt/code/prediction/batch_inference/session_manager.py,/opt/code/prediction/batch_inference/model_manager.py  /opt/code/prediction/batch_inference/usi_csv_batch_inference.py
