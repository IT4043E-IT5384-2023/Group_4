import requests
import os
from tqdm import tqdm
from utils import save_json, load_json

class QuestnAPI:
    def __init__(self):
        self.base_url = os.getenv("QUESTN_BASE_URL")
        self.headers = {"Content-Type": "application/json"}

    def make_request(self, endpoint, params=None):
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, params=params, headers=self.headers)
        return response


class QuestnCrawler:
    def __init__(self):
        self.api = QuestnAPI()

    def get_num_pages(self, endpoint, params=None):
        res = self.api.make_request(endpoint, params)
        if res.status_code == 200:
            return res.json()["result"]["num_pages"]
        else:
            return -1

    def crawl_quests(self):
        print("Getting quests on QuestN ...")
        quests = {}
        res = self.api.make_request(os.getenv("QUESTN_QUESTS_ENDPOINT"), {"count": 1})
        if res.status_code != 200:
            return quests
        count = res.json()["result"]["count"]
        res = self.api.make_request(
            os.getenv("QUESTN_QUESTS_ENDPOINT"),
            {
                "count": count,
                "page": 1,
                "search": "",
                "category": 100,
                "status_filter": 100,
                "community_filter": 0,
                "rewards_filter": 0,
                "chain_filter": 0,
                "user_id": 0,
            },
        )
        if res.status_code == 200:
            print("Called QuestN API")
            json_res = res.json()
            if json_res["success"]:
                data = json_res["result"]["data"]
                for quest in data:
                    if quest["id"] not in quests:
                        quests[quest["id"]] = quest
            else:
                print("Error, API call failed to be jsonified")
        else:
            print("Error: cannot call QuestN API")
        return quests

    def crawl_quest_users(self, quest_id):
        print(f"Getting quest {quest_id} users info ...")
        num_pages = self.get_num_pages(
            os.getenv("QUESTN_USERS_ENDPOINT"),
            {"quest_id": quest_id, "page": 1, "count": 1},
        )
        users = {}
        for page in tqdm(range(1, min(21, num_pages + 1))):
            res = self.api.make_request(
                os.getenv("QUESTN_USERS_ENDPOINT"),
                {"quest_id": quest_id, "page": page, "count": 24},
            )
            if res.status_code == 200:
                json_res = res.json()
                if json_res["success"]:
                    data = json_res["result"]["data"]
                    for user in data:
                        if user["user_id"] not in users:
                            users[user["user_id"]] = user
                else:
                    print("Error, API call failed to be jsonified")
            else:
                print("Error, cannot call the API")
        return users