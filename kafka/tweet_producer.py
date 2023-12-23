import sys

sys.path.append(".")

from argparse import ArgumentParser
from crawler import TweetCrawler
from kafka import KafkaProducer
from dotenv import load_dotenv
from utils import *
import os

load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_SERVER")
TOPIC = os.getenv("KAFKA_TWITTER_TOPIC")
GCS_PREFIX = os.getenv("GCS_PREFIX")
TWITTER_PASSWORD_PATH = os.getenv("TWITTER_PASSWORD_PATH")


def load_args():
    parser = ArgumentParser()
    with open(TWITTER_PASSWORD_PATH) as f:
        twitter_password = json.load(f)
    accs = range(len(twitter_password))
    parser.add_argument("--acc", type=int, default=0, choices=accs)
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


def get_keywords(project):
    keywords = [
        "_".join(project["_id"].split("-")),
        project["category"],
        project["source"],
    ]
    keywords = [k.lower() for k in keywords]
    keywords = " ".join(keywords)
    return keywords.strip().split(" ")

def process_tweets(tweets):
    p_tweets ={}
    for tweet in tweets:
        if tweet["id"] not in p_tweets and tweet._get_language() == "en":
            p_tweets[tweet["id"]] = {
                "id": tweet["id"],
                "text": tweet["text"] if tweet["text"] else "",
                "date": tweet["date"].timestamp() if tweet["date"] else 0,
                "hashtags": [str(h) for h in tweet["hashtags"]] if tweet["hashtags"] else [],
                "views": int(tweet["views"]) if tweet["views"] else 0,
                "reply_counts": tweet["reply_counts"] if tweet["reply_counts"] else 0,
                "retweet_counts": tweet["retweet_counts"] if tweet["retweet_counts"] else 0,
                "likes": tweet["likes"] if tweet["likes"] else 0,
                "is_sensitive": tweet["is_sensitive"],
            }
    return p_tweets


def main():
    args = load_args()
    topic = TOPIC + "_" + args.chain
    tweet_crawler = TweetCrawler(args.acc)
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    bucket = get_gc_bucket()
    prj_blob_path = os.path.join(
        GCS_PREFIX, "data/smart_contract", f"projects_{args.chain}.json"
    )
    print(prj_blob_path)
    projects = read_gc_json_blob(bucket, prj_blob_path)
    chunk = len(projects) // args.num_producer
    start = args.start * chunk
    end = (
        (args.start + 1) * chunk
        if args.start != args.num_producer - 1
        else len(projects)
    )
    print(
        f"Producer {args.start} will produce {start}-{end} projects' tweets of size {len(projects)}"
    )
    for name in list(projects.keys())[start:end]:
        keywords = get_keywords(projects[name])
        try:
            tweets = tweet_crawler.get_tweets_by_keywords(keywords)
        except:
            producer.send(topic, value=data)
            print("reconnecting")
            tweet_crawler.sign_in(tweet_crawler.account)
            tweets = tweet_crawler.get_tweets_by_keywords(keywords)
        if tweets:
            tweets = process_tweets(tweets)
            data = {
                "tweets": tweets,
            }
            producer.send(topic, value=data)
            print(f"Sent {len(tweets)} tweets of {name}")
    
    producer.send(topic, value={"end": 1/args.num_producer})
    producer.plush()

if __name__ == "__main__":
    main()