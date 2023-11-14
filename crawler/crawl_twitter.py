import json, os
from .utils import save_json
from tweety import Twitter

from tweety import Twitter

app = Twitter("session")
with open("crawler/password.json", "r") as f:
    data = json.load(f)
    username = data["username"]
    password = data["password"]
app.sign_in(username, password)
if not os.path.exists("data/twitter"):
    os.makedirs("data/twitter")

def scrape_tweets(username):
    print(f"Scrapping tweets from '{username}'")
    tweets = app.get_tweets(username, pages=100, wait_time=30)
    save_json(f"data/twitter/{username}.json", tweets)