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
    parser.add_argument("--start", type=int, default=None)
    parser.add_argument("--end", type=int, default=None)
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
        GCS_PREFIX, "data/smart_contract", f"projects_{args.chain}.json"
    )
    project_names = read_gc_json_blob(bucket, prj_blob_path)
    if args.start is None or args.start < 0:
        args.start = 0
    if args.end is None or args.end >= len(project_names):
        args.end = len(project_names) - 1
    for project_name in project_names[args.start : args.end]:
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

    producer.send(topic, value=0)
    producer.close()

if __name__ == "__main__":
    main()