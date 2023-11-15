from crawler import crawl_data
import argparse

def get_params():
    parser = argparse.ArgumentParser(description="Crawl data")
    parser.add_argument("--twitter_pwd", type=str, default="crawler/password.json", help="Path to twitter password file")
    parser.add_argument("--max_num_users", type=int, default=5000, help="Maximum number of users to crawl")
    return parser.parse_args()

if __name__ == "__main__":
    args = get_params()
    crawl_data(args)