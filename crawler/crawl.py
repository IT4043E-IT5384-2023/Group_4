from .crawl_questn import crawl_questn_quests, crawl_questn_quest_users, get_all_users
from .crawl_twitter import scrape_tweets
from .utils import save_json,load_json
import os

def main(args):
    print("Starting to crawl data")
    if not os.path.exists("data/questn/all_users.json"):
        if not os.path.exists("data/questn/quests.json"):
            quests = crawl_questn_quests()
        else:
            print("Loading quests from file")
            quests = load_json("data/questn/quests.json")
        all_users = {}
        for quest_id in quests.keys():
            users = crawl_questn_quest_users(quest_id)
            for user_id in users.keys():
                if user_id not in all_users:
                    all_users[user_id] = users[user_id]
            if len(all_users) > args.max_num_users:
                break
        save_json("data/questn/all_users.json", all_users)
    else:
        print("Loading users from file")
        all_users = get_all_users()
    for user_id in list(all_users.keys())[::-1]:
        if not f"{all_users[user_id]['twitter_username']}.json" in os.listdir("data/twitter"):
            scrape_tweets(all_users[user_id]["twitter_username"], args)