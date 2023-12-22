import sys

sys.path.append(".")
from argparse import ArgumentParser
from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils import *
import os, json

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
    return parser.parse_args()


def save_to_bucket(blob_name, data):
    bucket = get_gc_bucket()
    write_gc_json_blob(bucket, blob_name, data)


def load_from_bucket(blob_name):
    bucket = get_gc_bucket()
    return read_gc_json_blob(bucket, blob_name)


def main():
    args = load_args()
    topic = TOPIC + "_" + args.chain
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="smart-contract-consumer",
    )
    consumer.subscribe([topic])
    try:
        all_projects = load_from_bucket(
            os.path.join(
                GCS_PREFIX, "data/smart_contract", f"projects_{args.chain}.json"
            )
        )
        all_wallets = load_from_bucket(
            os.path.join(
                GCS_PREFIX, "data/smart_contract", f"wallets_{args.chain}.json"
            )
        )
    except:
        all_projects = {}
        all_wallets = {}
    count = 0
    for message in consumer:
        if message.value == 0:
            break
        name = message.value["name"]
        prj = message.value["prj"]
        addrs = message.value["addrs"]
        if name not in all_projects:
            count += 1
            print("Received ", name)
            all_projects[name] = prj
            for addr, prj_name in addrs.items():
                if addr not in all_wallets:
                    all_wallets[addr] = {prj_name: 1}
                else:
                    all_wallets[addr][prj_name] = all_wallets[addr].get(prj_name, 0) + 1
        if (count + 1) % 100 == 0:
            save_to_bucket(
                os.path.join(
                    GCS_PREFIX, "data/smart_contract", f"projects_{args.chain}.json"
                ),
                all_projects,
            )
            save_to_bucket(
                os.path.join(
                    GCS_PREFIX, "data/smart_contract", f"wallets_{args.chain}.json"
                ),
                all_wallets,
            )

    consumer.close()
    save_to_bucket(
        os.path.join(GCS_PREFIX, "data/smart_contract", f"projects_{args.chain}.json"),
        all_projects,
    )
    save_to_bucket(
        os.path.join(GCS_PREFIX, "data/smart_contract", f"wallets_{args.chain}.json"),
        all_wallets,
    )


if __name__ == "__main__":
    main()
