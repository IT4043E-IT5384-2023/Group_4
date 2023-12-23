import sys

sys.path.append(".")
from argparse import ArgumentParser
from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils import *
import os, json, math

load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_SERVER")
TOPIC = os.getenv("KAFKA_TWITTER_TOPIC")
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
        all_tweets = load_from_bucket(
            os.path.join(GCS_PREFIX, "data/tweet", f"tweets_{args.chain}.json")
        )
    except:
        all_tweets = {}
    end = 0
    count = 0
    for message in consumer:
        if "end" in message.value:
            end += message.value["end"]
        if abs(end - 1) < 1e-6:
            break
        tweets = message.value["tweets"]
        print(f"Received {len(tweets)} tweets")
        for id in tweets:
            if id not in all_tweets:
                count += 1
                all_tweets[id] = tweets[id]
        if (count + 1) % 100 == 0:
            save_to_bucket(
                os.path.join(GCS_PREFIX, "data/tweet", f"tweets_{args.chain}.json"),
                all_tweets,
            )

    consumer.close()
    save_to_bucket(
        os.path.join(GCS_PREFIX, "data/tweet", f"tweets_{args.chain}.json"),
        all_tweets,
    )

if __name__ == "__main__":
    main()
