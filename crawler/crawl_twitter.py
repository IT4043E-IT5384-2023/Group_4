import json, os, time
from .utils import save_json
from tweety import Twitter
import tweety

app = Twitter("session")
if not os.path.exists("data/twitter"):
    os.makedirs("data/twitter")

def scrape_tweets(username, args):
    if username == "":
        print("Empty username")
        return
    with open(args.twitter_pw, "r") as f:
        data = json.load(f)
        account = data["username"]
        password = data["password"]
        extra = data["extra"] if "extra" in data else ""
    if extra:
        app.sign_in(account, password, extra=extra)
    else:
        app.sign_in(account, password)
    print(f"Scrapping tweets from '{username}'")
    start = time.time()
    try:
        tweets = app.get_tweets(username, pages=100, wait_time=30)
        save_json(f"data/twitter/{username}.json", tweets)
        print(f"Done in {(time.time() - start)/3600:.2f} hours") 
    except tweety.exceptions_.UserNotFound:
        print(f"User '{username}' not found")
    except:
        print("Something went wrong")