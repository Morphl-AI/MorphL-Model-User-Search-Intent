touch /home/airflow/.morphl_usi_environment.sh
chmod 660 /home/airflow/.morphl_usi_environment.sh
chown airflow /home/airflow/.morphl_usi_environment.sh

echo "export USI_GOOGLE_CLOUD_PROJECT=morphl-cloud" >> /home/airflow/.morphl_usi_environment.sh
echo "export USI_GOOGLE_CLOUD_BUCKET=usi-csv-samples" >> /home/airflow/.morphl_usi_environment.sh
echo "export USI_GOOGLE_CLOUD_PROCESSED=jot/processed" >> /home/airflow/.morphl_usi_environment.sh
echo "export USI_GOOGLE_CLOUD_UNPROCESSED=jot/processed" >> /home/airflow/.morphl_usi_environment.sh
echo "export USI_GOOGLE_CLOUD_SERVICE_ACCOUNT=/opt/secrets/usi/gcloud_service_account.json" >> /home/airflow/.morphl_usi_environment.sh
echo ". /home/airflow/.morphl_usi_environment.sh" >> /home/airflow/.profile

