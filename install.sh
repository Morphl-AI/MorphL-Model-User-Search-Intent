echo 'Installing the User Search Intent Prediction module...'
echo

touch /home/airflow/.morphl_usi_csv_environment.sh
chmod 660 /home/airflow/.morphl_usi_csv_environment.sh
chown airflow /home/airflow/.morphl_usi_csv_environment.sh

echo "export USI_GOOGLE_CLOUD_PROJECT=morphl-cloud" >> /home/airflow/.morphl_usi_csv_environment.sh
echo "export USI_GOOGLE_CLOUD_BUCKET=usi-csv-samples" >> /home/airflow/.morphl_usi_csv_environment.sh
echo "export USI_GOOGLE_CLOUD_PROCESSED=jot/processed" >> /home/airflow/.morphl_usi_csv_environment.sh
echo "export USI_GOOGLE_CLOUD_UNPROCESSED=jot/unprocessed" >> /home/airflow/.morphl_usi_csv_environment.sh
echo "export USI_GOOGLE_CLOUD_SERVICE_ACCOUNT=/opt/secrets/usi_csv/gcloud_service_account.json" >> /home/airflow/.morphl_usi_csv_environment.sh
echo ". /home/airflow/.morphl_usi_csv_environment.sh" >> /home/airflow/.profile

mkdir -p /opt/secrets/usi_csv
touch /opt/secrets/usi_csv/gcloud_service_account.json
chmod -R 775 /opt/secrets/usi_csv
chmod 660 /opt/secrets/usi_csv/gcloud_service_account.json
chgrp airflow /opt/secrets/usi_csv /opt/secrets/usi_csv/gcloud_service_account.json

echo 'Initiating the Cassandra database...'
echo

cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} -f /opt/usi_csv/cassandra_schema/usi_csv_cassandra_schema.cql

echo 'Setting up the pipeline...'
echo

stop_airflow.sh
cp -rf /opt/usi_csv/pipeline/usi_csv_airflow_dag.py.template /home/airflow/airflow/dags/usi_csv_airflow_dag.py
start_airflow.sh