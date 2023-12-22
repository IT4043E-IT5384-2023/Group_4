import sys

sys.path.append(".")
from argparse import ArgumentParser
from crawler.SmartContractCrawler import SmartContractCrawler
from kafka import KafkaProducer
from dotenv import load_dotenv
from utils import *
import os
import json
import psycopg2

load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_SERVER")
TOPIC = os.getenv("KAFKA_SM_TOPIC")
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
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--num_producer", type=int, default=10)
    return parser.parse_args()

def main():
    args = load_args()
    topic = TOPIC + "_" + args.chain
    sm_cralwer = SmartContractCrawler(chain=args.chain)
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    bucket = get_gc_bucket()
    prj_blob_path = os.path.join(
        GCS_PREFIX, "data/project", f"{args.chain}.json"
    )
    project_names = list(read_gc_json_blob(bucket, prj_blob_path))
    chunk = len(project_names) // args.num_producer
    start = args.start * chunk
    end = (args.start + 1) * chunk if args.start != args.num_producer - 1 else len(project_names)
    print(f"Producer {args.start} will produce {start}-{end} projects of size {len(project_names)}")
    for project_name in project_names[start : end]:
        try:
            prj, addrs = sm_cralwer.extract(project_name)
        except psycopg2.OperationalError:
            producer.send(topic, value=data)
            sm_cralwer.connect()
            prj, addrs = sm_cralwer.extract(project_name)
        data = {
            "name": project_name,
            "prj": prj,
            "addrs": addrs,
        }
        producer.send(topic, value=data)
        print("Sent ", project_name)
    if end == len(project_names):
        producer.send(topic, value=0)
    producer.close()

if __name__ == "__main__":
    main()