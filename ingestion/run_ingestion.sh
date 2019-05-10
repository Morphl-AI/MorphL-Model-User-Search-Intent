cp -r /opt/usi_csv /opt/code
cd /opt/code
git pull


GCP_PROJECT_ID=$(jq -r '.project_id' ${USI_GOOGLE_CLOUD_SERVICE_ACCOUNT})
USI_LOCAL_PATH=/opt/landing/

gcloud config set project ${USI_GOOGLE_CLOUD_PROJECT}

gcloud auth activate-service-account --key-file=${USI_GOOGLE_CLOUD_SERVICE_ACCOUNT}

mkdir ${USI_LOCAL_PATH}

gsutil cp -r dir gs://${USI_GOOGLE_CLOUD_BUCKET}/${USI_GOOGLE_CLOUD_UNPROCESSED} ${USI_LOCAL_PATH}

export USI_LOCAL_PATH

spark-submit --jars /opt/spark/jars/spark-cassandra-connector.jar,/opt/spark/jars/jsr166e.jar  /opt/code/ingestion/ingestion.py

rm -r ${USI_LOCAL_PATH}

