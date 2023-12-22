from utils import *
from tqdm import tqdm
import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

MONGO_CONN_STR = os.getenv("MONGO_CONN_STR")
MONGO_KG_CONN_STR = os.getenv("MONGO_KG_CONN_STR")


class SmartContractCrawler:
    def __init__(self, chain):
        self.chain = chain
        self.connect()

    def connect(self):
        self.pg_conn = get_pg_conn(
            POSTGRES_DB,
            POSTGRES_HOST,
            POSTGRES_USER,
            POSTGRES_PASSWORD,
            POSTGRES_PORT,
        )
        mongo_client = get_mg_client(MONGO_CONN_STR)
        mongo_kg_client = get_mg_client(MONGO_KG_CONN_STR)
        self.transaction_db = mongo_client[get_chain_db(self.chain)].transactions
        self.projects_db = mongo_kg_client.knowledge_graph.projects

    def get_projects(self):
        # query for distinct project names
        print("Querying projects ...")
        query = f"SELECT DISTINCT project FROM {self.chain}.smart_contract;"
        cursor = self.pg_conn.cursor()
        cursor.execute(query)
        res = cursor.fetchall()
        if res is None:
            print(f"No project found in {self.chain}.smart_contract")
            return []
        res = [
            r[0]
            for r in res
            if self.projects_db.find_one({"_id": "-".join(r[0].split("_"))})
        ]
        print(f"Found {len(res)} projects in {self.chain}.smart_contract")
        return res

    def extract(self, project_name):
        query = f"SELECT contract_address FROM {self.chain}.smart_contract WHERE project = '{project_name}';"
        cursor = self.pg_conn.cursor()
        cursor.execute(query)
        res = cursor.fetchall()
        if res is None:
            print(f"No contract address found for {project_name}")
            return
        prj = self.projects_db.find_one({"_id": "-".join(project_name.split("_"))})
        addresses = {}
        for r in tqdm(res):
            count = 0
            to_addr = r[0]
            trans = self.transaction_db.find({"to_address": {"$eq": to_addr}})
            for tran in trans:
                count += 1
                addresses[tran["from_address"]] = project_name
            prj["numberOfContractsTransactions"] = (
                prj.get("numberOfContractsTransactions", 0) + count
            )
        return prj, addresses
