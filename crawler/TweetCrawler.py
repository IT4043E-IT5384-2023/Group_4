from dotenv import load_dotenv
import json, os
import tweety

load_dotenv()

TWITTER_PASSWORD_PATH = os.getenv("TWITTER_PASSWORD_PATH")
TWITTER_WAIT_TIME = os.getenv("TWITTER_WAIT_TIME")
TWITTER_PAGES = os.getenv("TWITTER_PAGES")
TWITTER_MIN_FAVORS = os.getenv("TWITTER_MIN_FAVORS")
class TweetCrawler:
    def __init__(self, account=0):
        self.account = account
        self.sign_in(self.account)

    def sign_in(self, acc):
        with open(os.getenv("TWITTER_PASSWORD_PATH"), "r") as f:
            data = json.load(f)
            data = data[acc]
            account = data["username"]
            password = data["password"]
            extra = data["extra"] if "extra" in data else ""
        self.app = tweety.Twitter("tw_session/" + account)
        if extra:
            self.app.sign_in(account, password, extra=extra)
        else:
            self.app.sign_in(account, password)

    def get_tweets_by_username(self, username):
        tweets = None
        try:
            tweets = self.app.get_tweets(
                username,
                pages=os.getenv("TWITTER_PAGES"),
                wait_time=os.getenv("TWITTER_WAIT_TIME"),
            )
        except tweety.exceptions_.UserNotFound:
            print(f"User '{username}' not found")
        except:
            print("Something went wrong")
        finally:
            return tweets

    def get_tweets_by_keywords(self, keywords):
        tweets = self.app.search(
            f"({keywords[0]} OR {keywords[1]} OR #{keywords[0]} OR #{keywords[1]} OR @{keywords[0]} OR @{keywords[1]}) min_faves:{TWITTER_MIN_FAVORS}",
            filter_=tweety.filter.SearchFilters.Latest(),
            pages=TWITTER_PAGES,
            wait_time=TWITTER_WAIT_TIME,
        )

        return tweets
