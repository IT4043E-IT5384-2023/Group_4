import json, os, time
from .utils import save_json
from tweety import Twitter
import tweety

app = Twitter("session")
with open("crawler/password.json", "r") as f:
    data = json.load(f)
    username = data["username"]
    password = data["password"]
app.sign_in(username, password)
if not os.path.exists("data/twitter"):
    os.makedirs("data/twitter")

def scrape_tweets(username):
    if username == "":
        print("Empty username")
        return
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