import json
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.compute import ComputeEngineStartInstanceOperator, ComputeEngineStopInstanceOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from datetime import timedelta, datetime

from dataloader.airflow_utils.slack import task_fail_slack_alert
from dataloader.scripts.populate_documentation import update_table_descriptions
from ror_scripts import fetch


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 7),
    "email": ["jennifer.melot@georgetown.edu"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert
}


with DAG("ror_updater",
            default_args=default_args,
            description="Links articles across our scholarly lit holdings.",
            schedule_interval="0 0 * * 5",
            catchup=False
         ) as dag:
    slack_webhook = BaseHook.get_connection("slack")
    bucket = "airflow-data-exchange"
    gcs_folder = "ror"
    tmp_dir = f"{gcs_folder}/tmp"
    raw_data_dir = f"{gcs_folder}/data"
    schema_dir = f"{gcs_folder}/schemas"
    sql_dir = f"sql/{gcs_folder}"
    production_dataset = "gcp_cset_ror"
    staging_dataset = "staging_"+production_dataset
    backup_dataset = production_dataset+"_backups"
    project_id = "gcp-cset-projects"
    gce_zone = "us-east1-c"
    dags_dir = os.environ.get("DAGS_FOLDER")

    # We keep several intermediate outputs in a tmp dir on gcs, so clean it out at the start of each run. We clean at
    # the start of the run so if the run fails we can examine the failed data
    clear_tmp_dir = GCSDeleteObjectsOperator(
        task_id="clear_tmp_gcs_dir",
        bucket_name=bucket,
        prefix=tmp_dir + "/"
    )

    # Retrieve and expand the data
    json_loc = tmp_dir+"/ror.json"
    fetch = PythonOperator(
        task_id="fetch",
        op_kwargs={
            "output_bucket": bucket,
            "output_loc": json_loc,
        },
        python_callable=fetch
    )

    # Load into GCS
    load_staging = GCSToBigQueryOperator(
        task_id="load_staging",
        bucket=bucket,
        source_objects=[json_loc],
        schema_object=f"{schema_dir}/ror.json",
        destination_project_dataset_table=f"{staging_dataset}.all_metadata_norm",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE"
    )

    # Check that the number of ids is >= what we have in production and that the ids are unique
    checks = [
        BigQueryCheckOperator(
            task_id="check_unique_ids",
            sql=(f"select count(distinct(id)) = count(id) from {staging_dataset}.ror"),
            use_legacy_sql=False
        ),
        BigQueryCheckOperator(
            task_id="check_monotonic_increase",
            sql=(f"select (select count(0) from {staging_dataset}.ror) >= "
                 f"(select count(0) from {production_dataset}.ror)"),
            use_legacy_sql=False
        )
    ]

    # Load into production
    load_production = BigQueryToBigQueryOperator(
        task_id=f"load_production",
        source_project_dataset_tables=[f"{staging_dataset}.ror"],
        destination_project_dataset_table=f"{production_dataset}.ror",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE"
    )

    # Update descriptions
    with open(f"{os.environ.get('DAGS_FOLDER')}/schemas/{gcs_folder}/table_descriptions.json") as f:
        table_desc = json.loads(f.read())
    pop_descriptions = PythonOperator(
        task_id="populate_column_documentation",
        op_kwargs={
            "input_schema": f"{os.environ.get('DAGS_FOLDER')}/schemas/{gcs_folder}/ror.json",
            "table_name": f"{production_dataset}.ror",
            "table_description": table_desc["ror"]
        },
        python_callable=update_table_descriptions
    )

    # Copy to backups
    curr_date = datetime.now().strftime("%Y%m%d")
    backup = BigQueryToBigQueryOperator(
        task_id=f"snapshot_ror",
        source_project_dataset_tables=[f"{production_dataset}.ror"],
        destination_project_dataset_table=f"{backup_dataset}.ror_{curr_date}",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE"
    )

    # Declare victory
    success_alert = SlackAPIPostOperator(
        task_id="post_success",
        token=slack_webhook.password,
        text="ROR update succeeded!",
        channel=slack_webhook.login,
        username="airflow"
    )

    clear_tmp_dir >> fetch >> load_staging >> checks >> load_production >> backup >> success_alert
