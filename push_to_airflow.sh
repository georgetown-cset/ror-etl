gsutil cp ror_dag.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/
gsutil -m cp -r ror_scripts/* gs://airflow-data-exchange/ror/scripts/
gsutil -m cp -r schemas/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/schemas/ror/
gsutil -m cp -r schemas gs://airflow-data-exchange/ror/
