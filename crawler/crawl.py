from .crawl_questn import crawl_questn_quests, crawl_questn_quest_users, get_all_users
from .crawl_twitter import scrape_tweets
from .utils import save_json

def main():
    quests = crawl_questn_quests()
    all_users = {}
    for quest_id in quests.keys():
        users = crawl_questn_quest_users(quest_id)
        for user in users.values():
            all_users.add(user)
        if len(all_users) > 5000:
            break
    save_json("data/questn/all_users.json", all_users)
    for user in all_users:
        scrape_tweets(user["username"])