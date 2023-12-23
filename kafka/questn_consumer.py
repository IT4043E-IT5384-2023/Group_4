import sys
sys.path.append(".")
from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils import *
import os, json

load_dotenv()

TOPIC = os.getenv("KAFKA_QUESTN_TOPIC")
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
GCS_PREFIX = os.getenv("GCS_PREFIX")


def main():
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="questn-consumer",
    )
    consumer.subscribe([TOPIC])
    all_users = {}
    for message in consumer:
        if message.value == 0:
            break
        quest_id, users = list(message.value.items())[0]

        print(f"Received {len(users)} users from {quest_id}")
        for user_id in users:
            if user_id not in all_users:
                all_users[user_id] = users[user_id]
    blob_path = os.path.join(GCS_PREFIX, "data/questn/all_users.json")
    bucket = get_gc_bucket()
    write_gc_json_blob(bucket, blob_path, all_users)
    consumer.close()


if __name__ == "__main__":
    main()
