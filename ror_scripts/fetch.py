import argparse
import json
import os
import requests
import tempfile

from google.cloud import storage
from zipfile import ZipFile


def fetch(output_bucket: str, output_loc: str) -> None:
    """
    With thanks to https://ror.readme.io/docs/data-dump, fetches ROR data,
    converts to jsonl, and uploads to GCS
    :param output_bucket: GCS bucket where json of data should be written
    :param output_loc: blob name where data should be written on GCS
    :return: None
    """
    resp = requests.get("https://zenodo.org/api/records/?communities=ror-data&sort=mostrecent")
    dataset_js = resp.json()
    latest_delivery = dataset_js["hits"]["hits"][0]["files"][0]["links"]["self"]
    zip_resp = requests.get(latest_delivery)
    with tempfile.TemporaryDirectory() as td:
        zip_f = os.path.join(td, latest_delivery.split("/")[-1])
        print(zip_f)
        with open(zip_f, mode="wb") as f:
            f.write(zip_resp.content)
        ZipFile(zip_f).extractall(td)
        print(f"Downloaded content: {os.listdir(td)}")
        json_files = [js for js in os.listdir(td) if js.endswith(".json")]
        assert len(json_files) == 1
        output_file = os.path.join(td, output_loc.split("/")[-1])
        with open(os.path.join(td, json_files[0])) as f:
            js = json.loads(f.read())
            with open(output_file, mode="w") as out:
                for elt in js:
                    out.write(json.dumps(elt)+"\n")
        storage_client = storage.Client()
        bucket = storage_client.bucket(output_bucket)
        blob = bucket.blob(output_loc)
        blob.upload_from_filename(output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_bucket", help="GCS bucket where data should be written", required=True)
    parser.add_argument("--output_loc", help="Blob name where data shuld be written", required=True)
    args = parser.parse_args()

    fetch(args.output_bucket, args.output_loc)
