from crawler import crawl_data
import argparse

def get_params():
    parser = argparse.ArgumentParser(description="Crawl data")
    parser.add_argument("--twitter_pw", type=str, default="crawler/password.json", help="Path to twitter password file")
    parser.add_argument("--max_num_users", type=int, default=5000, help="Maximum number of users to crawl")
    parser.add_argument("--num_pages", type=int, default=100, help="Maximum number of pages to crawl")
    parser.add_argument("--wait_time", type=int, default=30, help="Wait time between each request")
    parser.add_argument("--start", type=int, default=0, help="Start index")
    parser.add_argument("--end", type=int, default=5000, help="End index")
    return parser.parse_args()

if __name__ == "__main__":
    args = get_params()
    crawl_data(args)