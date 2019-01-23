cp -r /opt/usi_csv /opt/code
cd /opt/code
git pull
python /opt/code/prediction/model_serving/model_serving_endpoint.py

