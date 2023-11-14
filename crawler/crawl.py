from .crawl_questn import crawl_questn_quests, crawl_questn_quest_users, get_all_users
from .crawl_twitter import scrape_tweets
from .utils import save_json,load_json
import os

def main():
    if not os.path.exists("data/questn/quests.json"):
        quests = crawl_questn_quests()
    else:
        quests = load_json("data/questn/quests.json")
    if not os.path.exists("data/questn/all_users.json"):
        all_users = {}
        for quest_id in quests.keys():
            users = crawl_questn_quest_users(quest_id)
            for user_id in users.keys():
                if user_id not in all_users:
                    all_users[user_id] = users[user_id]
            if len(all_users) > 5000:
                break
        save_json("data/questn/all_users.json", all_users)
    else:
        all_users = get_all_users()
    for user in all_users.values():
        if not f"{user['twitter_username']}.json" in os.listdir("data/twitter"):
            scrape_tweets(user["twitter_username"])