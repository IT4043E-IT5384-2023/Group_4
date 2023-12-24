# Big Data Storage and Processing project

# Group 4 - Problem 2

## Installation requirement:

Firstly, you need to run the installation `requirements.txt`:

```
pip install -r requirements.txt
```

## Crawling QuestN's users:

Run 2 following kafka files in parallel:

```
python3 kafka/questn_consumer.py
```

```
python3 kakfa/questn_producer.py
```

## Crawling data from Twitter:

Before crawling Twitter data, `tweety` module requires a sign in method, please add a file "password.json" and its path into ".env" file. The format of the password file should look like this:

```
{
    {
        "username": "<your_twitter_username>",
        "password": "<your_twitter_password>",
        "extra": "<authentical_key>"
    }
}
```

Please notice that the `"extra"` value in the password file is the authentical key:

-   For the fist running, set `"extra"` to empty string value `""`.
    Then, run the crawling code:

```
python3 kafka/tweet_consumer.py --chain <chain>
```

-   In case you cannot run the below code and get the error:
    `tweety.exceptions_.ActionRequired: In order to protect your account from suspicious activity, we've sent a confirmation code to ***@gmail.com`.
    Get the key from your mail and set to `"extra"` value, and try to run the crawling code again.

-   There may be some interuptions during the crawling phrase due to connection or some other factors. Continue to run the command to keep crawling data from Twitter

## Crawling data from PostgresQL and MongoDB:

First we need to extract projects' name from PostgresQL by running following commands.

Note that `chain` variable is one of `["chain_0x1", "chain_0x38", "chain_0x89", "chain_0xfa", "chain_0xa4b1", "chain_0xa", "chain_0xa86a",]`

```
python3 kafka/project_producer.py --chain <chain>
```

```
python3 kafka/project_consumer.py --chain <chain>
```

Then run the following commands to extract projects with contracts:

```
python3 kafka/smart_contract_consumer.py --chain <chain>
```

```
python3 kafka/smart_contract_producer.py --chain <chain> --start <start> --num_producer <num>
```

Note that the `start` and `num_producer` are used to divide data into many KafkaProducer of many processes running in parallel. By default, one process will be the only producer.
