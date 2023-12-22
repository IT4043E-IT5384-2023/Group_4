import sys

sys.path.append(".")
from argparse import ArgumentParser
from crawler import SmartContractCrawler
from kafka import KafkaProducer
from dotenv import load_dotenv
import os, json

load_dotenv()

TOPIC = os.getenv("KAFKA_PRJ_TOPIC")
KAFKA_SERVER = os.getenv("KAFKA_SERVER")


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
    crawler = SmartContractCrawler(chain=args.chain)
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    projects = crawler.get_projects()
    producer.send(topic, value={"prj": projects})
    producer.send(topic, value=0)
    producer.close()


if __name__ == "__main__":
    main()
