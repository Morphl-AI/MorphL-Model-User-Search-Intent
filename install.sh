#!/usr/bin/env bash
echo '***** Installing the User Search Intent Prediction module... *****'

echo '-------------------------------------------------------'
echo 'Setting up environment variables...'
echo '-------------------------------------------------------'

touch /home/airflow/.morphl_usi_csv_environment.sh
chmod 660 /home/airflow/.morphl_usi_csv_environment.sh
chown airflow /home/airflow/.morphl_usi_csv_environment.sh

echo "export USI_GOOGLE_CLOUD_PROJECT=morphl-cloud" >> /home/airflow/.morphl_usi_csv_environment.sh
echo "export USI_GOOGLE_CLOUD_BUCKET=usi-csv-samples" >> /home/airflow/.morphl_usi_csv_environment.sh
echo "export USI_GOOGLE_CLOUD_PROCESSED=jot/processed" >> /home/airflow/.morphl_usi_csv_environment.sh
echo "export USI_GOOGLE_CLOUD_UNPROCESSED=jot/unprocessed" >> /home/airflow/.morphl_usi_csv_environment.sh
echo "export USI_GOOGLE_CLOUD_SERVICE_ACCOUNT=/opt/secrets/usi_csv/gcloud_service_account.json" >> /home/airflow/.morphl_usi_csv_environment.sh

if ! grep -q "/home/airflow/.morphl_usi_csv_environment.sh" /home/airflow/.profile; then
  echo ". /home/airflow/.morphl_usi_csv_environment.sh" >> /home/airflow/.profile
fi


mkdir -p /opt/secrets/usi_csv
touch /opt/secrets/usi_csv/gcloud_service_account.json
chmod -R 775 /opt/secrets/usi_csv
chmod 660 /opt/secrets/usi_csv/gcloud_service_account.json
chgrp airflow /opt/secrets/usi_csv /opt/secrets/usi_csv/gcloud_service_account.json


echo '-------------------------------------------------------'
echo 'Initiating the Cassandra database...'
echo '-------------------------------------------------------'

cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} -f /opt/usi_csv/cassandra_schema/usi_csv_cassandra_schema.cql


echo '-------------------------------------------------------'
echo 'Initiating the User Search Intent service...'
echo '-------------------------------------------------------'



kubectl apply -f /opt/usi_csv/prediction/model_serving/usi_csv_kubernetes_deployment.yaml
kubectl apply -f /opt/usi_csv/prediction/model_serving/usi_csv_kubernetes_service.yaml
USI_CSV_KUBERNETES_CLUSTER_IP_ADDRESS=$(kubectl get service/usi-csv-service -o jsonpath='{.spec.clusterIP}')

if ! grep -q "kubernetes-upstream-usi-csv" /opt/dockerbuilddirs/apicontainervolume/etc/nginx/conf.d/upstream.conf; then
  echo -e "upstream kubernetes-upstream-usi-csv { server ${USI_CSV_KUBERNETES_CLUSTER_IP_ADDRESS}; } \n" >> /opt/dockerbuilddirs/apicontainervolume/etc/nginx/conf.d/upstream.conf
fi

if ! grep -q "kubernetes-upstream-usi-csv" /opt/dockerbuilddirs/apicontainervolume/etc/nginx/api.conf; then
  sed '/^### additional locations placeholder ###/i # User Search Intent with CSV files\nlocation ^~ /search-intent {\n\tproxy_pass http://kubernetes-upstream-usi-csv;\n}\n' /opt/dockerbuilddirs/apicontainervolume/etc/nginx/api.conf
fi

docker restart -t 5 apicontainer


echo '-------------------------------------------------------'
echo 'Setting up the pipeline...'
echo '-------------------------------------------------------'

stop_airflow.sh
cp -rf /opt/usi_csv/pipeline/usi_csv_airflow_dag.py.template /home/airflow/airflow/dags/usi_csv_airflow_dag.py
start_airflow.sh


echo '-------------------------------------------------------'
echo 'Testing the API...'
echo '-------------------------------------------------------'

curl -s http://${USI_CSV_KUBERNETES_CLUSTER_IP_ADDRESS}/search-intent
