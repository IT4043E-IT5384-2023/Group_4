import sys
sys.path.append(".")
from crawler.QuestnCrawler import QuestnCrawler
from kafka import KafkaProducer
from dotenv import load_dotenv
import os, json
load_dotenv()

TOPIC = os.getenv("KAFKA_QUESTN_TOPIC")
KAFKA_SERVER = os.getenv("KAFKA_SERVER")

def main():
    questn_crawler = QuestnCrawler()
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    quests = questn_crawler.crawl_quests()
    for quest_id in quests:
        users = questn_crawler.crawl_quest_users(quest_id)
        print(f"Sent {len(users)} users of quest {quest_id}")
        producer.send(TOPIC, value={quest_id: users})

    producer.send(TOPIC, value=0)
    producer.close()

if __name__ == "__main__":
    main()