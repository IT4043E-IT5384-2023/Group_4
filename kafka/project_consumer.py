import sys

sys.path.append(".")

from argparse import ArgumentParser
from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils import *
import os, json

load_dotenv()

TOPIC = os.getenv("KAFKA_PRJ_TOPIC")
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
GCS_PREFIX = os.getenv("GCS_PREFIX")


def load_args():
    parser = ArgumentParser()
    valid_chains = [
        "chain_0x1",
        "chain_0x38",
        "chain_0x89",
        "chain_0xfa",
        "chain_0xa4b1",
        "chain_0xa",
        "chain_0xa86a",
    ]
    parser.add_argument("--chain", type=str, required=True, choices=valid_chains)
    return parser.parse_args()


def main():
    args = load_args()
    topic = TOPIC + "_" + args.chain
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="project-consumer",
    )
    consumer.subscribe([topic])
    projects = {}
    for message in consumer:
        if message.value == 0:
            break
        projects = message.value["prj"]
        print(f"Received {len(projects)} projects")

    consumer.close()

    blob_path = os.path.join(GCS_PREFIX, "data/project", f"{args.chain}.json")
    bucket = get_gc_bucket()
    write_gc_json_blob(bucket, blob_path, projects)


if __name__ == "__main__":
    main()
