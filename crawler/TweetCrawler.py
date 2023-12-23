from dotenv import load_dotenv
import json, os, time
from tweety import Twitter
from tweety.filters import SearchFilters
from tweety.exceptions_ import UserNotFound

load_dotenv()

TWITTER_PASSWORD_PATH = os.getenv("TWITTER_PASSWORD_PATH")
TWITTER_WAIT_TIME = os.getenv("TWITTER_WAIT_TIME")
TWITTER_PAGES = os.getenv("TWITTER_PAGES")
TWITTER_MIN_FAVORS = os.getenv("TWITTER_MIN_FAVORS")

if not os.path.exists("tw_session"):
    os.mkdir("tw_session")

class TweetCrawler:
    def __init__(self, account: int = 0):
        self.account = account
        self.sign_in(self.account)

    def sign_in(self, acc):
        with open(TWITTER_PASSWORD_PATH, "r") as f:
            data = json.load(f)
            data = data[acc]
            account = data["username"]
            password = data["password"]
            extra = data["extra"] if "extra" in data else ""
        self.app = Twitter("tw_session/" + account)
        if extra:
            self.app.sign_in(account, password, extra=extra)
        else:
            self.app.sign_in(account, password)

    def get_tweets_by_username(self, username):
        tweets = None
        try:
            tweets = self.app.get_tweets(
                username,
                pages=TWITTER_PAGES,
                wait_time=TWITTER_WAIT_TIME,
            )
        except UserNotFound:
            print(f"User '{username}' not found")
        except:
            print("Something went wrong")
        finally:
            return tweets

    def get_tweets_by_keywords(self, keywords):
        query = "("
        for keyword in keywords:
            query += f"{keyword} OR #{keyword} OR @{keyword} OR "
        query = query[:-4] + f") min_faves:{TWITTER_MIN_FAVORS}"
        s = time.time()
        tweets = self.app.search(
            query,
            filter_=SearchFilters.Latest(),
            pages=TWITTER_PAGES,
            wait_time=TWITTER_WAIT_TIME,
        )
        print(f"Search with keywords: {keywords} in {time.time() - s:.2f}s")
        return tweets
