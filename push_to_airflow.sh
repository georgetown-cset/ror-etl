gsutil cp ror_dag.py gs://us-east1-production2023-87afc3c5-bucket/dags/
gsutil -m cp -r ror_scripts gs://us-east1-production2023-87afc3c5-bucket/dags/
gsutil -m cp -r schemas/* gs://us-east1-production2023-87afc3c5-bucket/dags/schemas/ror/
gsutil -m cp -r schemas gs://airflow-data-exchange/ror/
