import json
from datetime import datetime

from airflow import DAG
from airflow.lineage.entities import File
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from dataloader.airflow_utils.defaults import (
    DAGS_DIR,
    DATA_BUCKET,
    GCP_ZONE,
    PROJECT_ID,
    get_default_args,
    get_post_success,
)
from dataloader.scripts.populate_documentation import update_table_descriptions

args = get_default_args(pocs=["Jennifer"])
args["retries"] = 1


with DAG(
    "ror_updater",
    default_args=args,
    description="Updates ROR dataset.",
    schedule_interval="0 0 * * 5",
    catchup=False,
) as dag:
    gcs_folder = "ror"
    tmp_dir = f"{gcs_folder}/tmp"
    raw_data_dir = f"{gcs_folder}/data"
    schema_dir = f"{gcs_folder}/schemas"
    sql_dir = f"sql/{gcs_folder}"
    production_dataset = "gcp_cset_ror"
    staging_dataset = "staging_" + production_dataset
    backup_dataset = production_dataset + "_backups"

    # We keep several intermediate outputs in a tmp dir on gcs, so clean it out at the start of each run. We clean at
    # the start of the run so if the run fails we can examine the failed data
    clear_tmp_dir = GCSDeleteObjectsOperator(
        task_id="clear_tmp_gcs_dir", bucket_name=DATA_BUCKET, prefix=tmp_dir + "/"
    )

    # Retrieve and expand the data
    raw_jsonl_loc = tmp_dir + "/ror.jsonl"
    working_dir = "ror_working_dir"
    setup_commands = f"rm -rf {working_dir};" + " && ".join(
        [
            f"mkdir {working_dir}",
            f"cd {working_dir}",
            f"gsutil -m cp -r gs://{DATA_BUCKET}/{gcs_folder}/scripts/* .",
            "virtualenv venv",
            ". venv/bin/activate",
            "python3 -m pip install google-cloud-storage",
        ]
    )
    raw_gcs_lineage_file = File(url=f"gs://{DATA_BUCKET}/{raw_jsonl_loc}")
    download_data = GKEStartPodOperator(
        task_id="download_data",
        name="ror-download",
        project_id=PROJECT_ID,
        location=GCP_ZONE,
        cluster_name="cc2-task-pool",
        do_xcom_push=True,
        cmds=["/bin/bash"],
        arguments=[
            "-c",
            (
                setup_commands
                + f" && python3 fetch.py --output_bucket '{DATA_BUCKET}' --output_loc '{raw_jsonl_loc}'"
            ),
        ],
        inlets=[
            File(
                url="https://zenodo.org/api/records/?communities=ror-data&sort=mostrecent"
            )
        ],
        outlets=[raw_gcs_lineage_file],
        namespace="default",
        image=f"gcr.io/{PROJECT_ID}/cc2-task-pool",
        get_logs=True,
        startup_timeout_seconds=300,
        on_finish_action="delete_pod",
        affinity={
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "cloud.google.com/gke-nodepool",
                                    "operator": "In",
                                    "values": [
                                        "default-pool",
                                    ],
                                }
                            ]
                        }
                    ]
                }
            }
        },
    )

    jsonl_with_up = f"{tmp_dir}/ror_json_with_up.jsonl"
    add_ultimate_parent = GKEStartPodOperator(
        task_id="add_ultimate_parent",
        name="ror-ultimate-parent",
        project_id=PROJECT_ID,
        location=GCP_ZONE,
        cluster_name="cc2-task-pool",
        do_xcom_push=True,
        cmds=["/bin/bash"],
        arguments=[
            "-c",
            (
                setup_commands
                + f" && python3 get_ultimate_parent.py --bucket '{DATA_BUCKET}' --input_loc '{raw_jsonl_loc}' --output_loc '{jsonl_with_up}'"
            ),
        ],
        inlets=[raw_gcs_lineage_file],
        outlets=[File(url=f"gs://{DATA_BUCKET}/{jsonl_with_up}")],
        namespace="default",
        image=f"gcr.io/{PROJECT_ID}/cc2-task-pool",
        get_logs=True,
        startup_timeout_seconds=300,
        on_finish_action="delete_pod",
        affinity={
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "cloud.google.com/gke-nodepool",
                                    "operator": "In",
                                    "values": [
                                        "default-pool",
                                    ],
                                }
                            ]
                        }
                    ]
                }
            }
        },
    )

    load_staging = GCSToBigQueryOperator(
        task_id="load_staging",
        bucket=DATA_BUCKET,
        source_objects=[jsonl_with_up],
        schema_object=f"{schema_dir}/ror.json",
        destination_project_dataset_table=f"{staging_dataset}.ror",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    # Check that the number of ids is >= what we have in production and that the ids are unique
    checks = [
        BigQueryCheckOperator(
            task_id="check_unique_ids",
            sql=(f"select count(distinct(id)) = count(id) from {staging_dataset}.ror"),
            use_legacy_sql=False,
        ),
        BigQueryCheckOperator(
            task_id="check_monotonic_increase",
            sql=(
                f"select (select count(0) from {staging_dataset}.ror) >= "
                f"(select count(0) from {production_dataset}.ror)"
            ),
            use_legacy_sql=False,
        ),
    ]

    load_production = BigQueryToBigQueryOperator(
        task_id="load_production",
        source_project_dataset_tables=[f"{staging_dataset}.ror"],
        destination_project_dataset_table=f"{production_dataset}.ror",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    # Update column descriptions
    with open(f"{DAGS_DIR}/schemas/{gcs_folder}/table_descriptions.json") as f:
        table_desc = json.loads(f.read())
    pop_descriptions = PythonOperator(
        task_id="populate_column_documentation",
        op_kwargs={
            "input_schema": f"{DAGS_DIR}/schemas/{gcs_folder}/ror.json",
            "table_name": f"{production_dataset}.ror",
            "table_description": table_desc["ror"],
        },
        python_callable=update_table_descriptions,
    )

    # Copy to backups
    curr_date = datetime.now().strftime("%Y%m%d")
    backup = BigQueryToBigQueryOperator(
        task_id="snapshot_ror",
        source_project_dataset_tables=[f"{production_dataset}.ror"],
        destination_project_dataset_table=f"{backup_dataset}.ror_{curr_date}",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    # Declare victory
    success_alert = get_post_success("ROR update succeeded!", dag)

    (
        clear_tmp_dir
        >> download_data
        >> add_ultimate_parent
        >> load_staging
        >> checks
        >> load_production
        >> pop_descriptions
        >> backup
        >> success_alert
    )
