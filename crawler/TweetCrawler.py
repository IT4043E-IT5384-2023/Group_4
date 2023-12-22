import json, os, time
from utils import save_json
from tweety import Twitter
import tweety

class TweetCrawler:
    def __init__(self):
        with open(os.getenv("TWITTER_PASSWORD_PATH"), "r") as f:
            data = json.load(f)
            account = data["username"]
            password = data["password"]
            extra = data["extra"] if "extra" in data else ""
        self.app = tweety.Twitter(account)
        if extra:
            self.app.sign_in(account, password, extra=extra)
        else:
            self.app.sign_in(account, password)

    def get_tweets(self, username):
        tweets = None
        try:
            tweets = self.app.get_tweets(username, pages=os.getenv("TWITTER_PAGES"), wait_time=os.getenv("TWITTER_WAIT_TIME"))
        except tweety.exceptions_.UserNotFound:
            print(f"User '{username}' not found")
        except:
            print("Something went wrong")
        finally:
            return tweets