import argparse
import json
import os
import tempfile

from google.cloud import storage


def traverse_parents(ror_id: str, id_to_parent: dict) -> str:
    """
    Find the ultimate ancestor of a ror id
    :param ror_id: ror id to find the ultimate ancestor for
    :param id_to_parent: dict mapping ror id to parent ror id
    :return: Ultimate ancestor of a ror id
    """
    parent = id_to_parent[ror_id]
    if ror_id == parent:
        return ror_id
    return traverse_parents(parent, id_to_parent)


def roll_up(id_to_parent: dict) -> dict:
    """
    Roll up ror id to its ultimate parent.
    :param id_to_parent: ror id to immediate parent mapping. If a ROR id has no parent, it is mapped to itself
    :return: dict containing ror id to ultimate parent mapping. Ultimate parents are mapped to themselves
    """
    id_to_ultimate_parent = {}
    for ror_id in id_to_parent:
        ultimate_parent = traverse_parents(ror_id, id_to_parent)
        id_to_ultimate_parent[ror_id] = ultimate_parent
    return id_to_ultimate_parent


def run(bucket_name: str, input_loc: str, output_loc: str) -> None:
    """
    With thanks to https://ror.readme.io/docs/data-dump, fetches ROR data,
    converts to jsonl, and uploads to GCS
    :param bucket_name: GCS bucket where json of data should be read and written
    :param input_loc: Location of input data within `bucket`
    :param output_loc: Location where output data should be written to `bucket`
    :return: None
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    with tempfile.TemporaryDirectory() as td:
        blob = bucket.blob(input_loc)
        input_fi = os.path.join(td, "input.jsonl")
        blob.download_to_filename(input_fi)
        id_to_parent = {}
        rows = []
        with open(input_fi) as f:
            for line in f:
                js = json.loads(line)
                ror_id = js["id"]
                parent_id = ror_id
                for relationship in js["relationships"]:
                    if relationship["type"].lower() == "parent":
                        parent_id = relationship["id"]
                assert ror_id not in id_to_parent, f"Duplicate ID: {ror_id}"
                id_to_parent[ror_id] = parent_id
                rows.append(js)
        id_to_ultimate_parent = roll_up(id_to_parent)
        output_fi = os.path.join(td, "output.jsonl")
        with open(output_fi, mode="w") as out:
            for row in rows:
                row["ultimate_parent"] = id_to_ultimate_parent[row["id"]]
                out.write(json.dumps(row) + "\n")
        blob = bucket.blob(output_loc)
        blob.upload_from_filename(output_fi)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bucket",
        help="GCS bucket where data should be read and written",
        required=True,
    )
    parser.add_argument(
        "--input_loc", help="Location of input data within `bucket`", required=True
    )
    parser.add_argument(
        "--output_loc",
        help="Location where output data should be written to `bucket`",
        required=True,
    )
    args = parser.parse_args()

    run(args.bucket, args.input_loc, args.output_loc)
