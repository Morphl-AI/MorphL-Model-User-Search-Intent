cp -r /opt/usi_csv /opt/code
cd /opt/code
git pull


GCP_PROJECT_ID=$(jq -r '.project_id' ${USI_GOOGLE_CLOUD_SERVICE_ACCOUNT})

gcloud config set project ${USI_GOOGLE_CLOUD_PROJECT}

gcloud auth activate-service-account --key-file=${USI_GOOGLE_CLOUD_SERVICE_ACCOUNT}

gsutil cp -r dir gs://${USI_GOOGLE_CLOUD_BUCKET}/${USI_GOOGLE_CLOUD_UNPROCESSED} /opt/landing
python /opt/code/ingestion/ingestion.py

