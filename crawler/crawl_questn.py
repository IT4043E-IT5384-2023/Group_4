import requests, os
from tqdm import tqdm
from .utils import save_json, load_json

api = {
    "questn_get_quests": "https://api.questn.com/consumer/explore/list/?count={}&page=1&search=&category=100&status_filter=100&community_filter=0&rewards_filter=0&chain_filter=0&user_id=0",
    "questn_get_quest_users": "https://api.questn.com/consumer/quest/user_participants/?quest_id={}&page={}&count={}",
}
if not os.path.exists("data/questn/quest_users"):
    os.makedirs("data/questn/quest_users")

def get_count(api):
    res = requests.get(api)
    if res.status_code == 200:
        return res.json()["result"]["count"]
    else:
        return -1

def get_num_pages(api):
    res = requests.get(api)
    if res.status_code == 200:
        return res.json()["result"]["num_pages"]
    else:
        return -1


def crawl_questn_quests():
    print("Getting quests on QuestN ...", end="")
    count = get_count(api["questn_get_quests"].format(1))
    print("found {} quests".format(count))
    quests = {}
    res = requests.get(api["questn_get_quests"].format(count))
    if res.status_code :
        print("Successfully call questn api")
        json_res = res.json()
        if json_res["success"] == True:
            data = json_res["result"]["data"]
            for quest in tqdm(data):
                if quest["id"] not in quests:
                    quests[quest["id"]] = quest
            save_json("data/questn/quests.json", quests)
        else:
            print("Error, api call failed to be jsonified")
    else:
        print("Error: cannot call questn api")
    return quests

def crawl_questn_quest_users(quest_id):
    print(f"Getting quest {quest_id} users info ...")
    num_pages = get_num_pages(api["questn_get_quest_users"].format(quest_id, 1, 1))
    users = {}
    for page in tqdm(range(1, min(50, num_pages+1))):
        res = requests.get(api["questn_get_quest_users"].format(quest_id, page, 24))
        if res.status_code :
            json_res = res.json()
            if json_res["success"] == True:
                data = json_res["result"]["data"]
                for user in data:
                    if user["user_id"] not in users:
                        users[user["user_id"]] = user
            else:
                print("Error, api call failed to be jsonified")       
        else:
            print("Error, cannot call the api")
    print("\tFound {} users".format(len(users)))
    save_json("data/questn/quest_users/{}.json".format(quest_id), users)
    return users
    
def get_quest_ids():
    quests = load_json("data/questn/quests.json")
    return list(quests.keys())

def get_users(quest_id):
    users = load_json("data/questn/quest_users/{}.json".format(quest_id))
    return users

def get_all_users():
    users = load_json("data/questn/all_users.json")
    return users