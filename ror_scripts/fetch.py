import os
import requests
import tempfile

from google.cloud import storage
from zipfile import ZipFile


def fetch(output_bucket: str, output_loc: str):
    """
    With thanks to https://ror.readme.io/docs/data-dump, fetches ROR data
    and uploads to GCS
    :param output_bucket: GCS bucket where json of data should be written
    :param output_loc:
    :return:
    """
    resp = requests.get("https://zenodo.org/api/records/?communities=ror-data&sort=mostrecent")
    js = resp.json()
    latest_delivery = js["hits"]["hits"][0]["files"][0]["links"]["self"]
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
        storage_client = storage.Client()
        bucket = storage_client.bucket(output_bucket)
        blob = bucket.blob(output_loc)
        blob.upload_from_filename(os.path.join(td, json_files[0]))
