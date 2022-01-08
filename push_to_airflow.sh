gsutil cp ror_dag.py gs://us-east1-production-41653310-bucket/dags/
gsutil -m cp -r ror_scripts gs://us-east1-production-41653310-bucket/dags/
gsutil -m cp -r schemas/* gs://us-east1-production-41653310-bucket/dags/schemas/ror/
gsutil -m cp -r schemas gs://airflow-data-exchange/ror/
