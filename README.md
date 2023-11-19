# Big Data Storage and Processing project

## Group 4 - Problem 2

#### Installation requirement:

Firstly, you need to run the installation `requirements.txt`:

```
pip install -r requirements.txt
```

#### Crawling data from Twitter & QuestN:

For more information, please read [`crawler/README.md`](crawler/README.md)

Before crawling data, `tweety` module requires a sign in method, please add a file "password.json" into `crawler`. The format of the password file should look like this:

```
{
    "username": "<your_twitter_username>",
    "password": "<your_twitter_password>",
    "extra": "<authentical_key>"
}
```

Please notice that the `"extra"` value in the password file is the authentical key:

-   For the fist running, set `"extra"` to empty string value `""`.
    Then, run the crawling code:

```
python3 crawl_data.py
```

-   In case you cannot run the below code and get the error:
    `tweety.exceptions_.ActionRequired: In order to protect your account from suspicious activity, we've sent a confirmation code to ***@gmail.com`.
    Get the key from your mail and set to `"extra"` value, and try to run the crawling code again.

There are some options for `crawl_data.py`:
- `--twitter_pw`: the path to twitter password file.
- `--max_num_users`: the number of users needed to crawl.
- `--num_pages`: the number of pages for crawling each user's twitter
- `--wait_time`: the amount of time waiting between requests
