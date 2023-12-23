from pathlib import Path
import json
import pandas as pd
from tqdm import tqdm

def get_data(path):
  with open(path, encoding = 'utf8') as f:
    data = json.load(f)
    return data
  
def get_data_classify_hashtags(data, data_dictionary):
    if 'tweets' in data:
      tweets = data['tweets']
      for i in range(len(tweets)):
          tweet = tweets[i]
          if 'hashtags' in tweet:
            if len(tweet['hashtags']) > 0 and tweet['hashtags'] is not None:
                hashtags = tweet['hashtags'][0]['text']
                # if tweet['views'] is not None and tweet['views'] != '':
                #     views = tweet['views']
                # else:
                #     views = None
                # language
                if tweet['language'] is not None and tweet['language'] != '':
                    language = tweet['language']
                else:
                    language = None
                # likes count of tweet
                if tweet['likes'] is not None and tweet['likes'] != '':
                    likeCount = tweet['likes']
                else:
                    likeCount = None
                # content of tweet
                if tweet['text'] is not None and tweet['text'] != '':
                    rawContent = tweet['text']
                else:
                    rawContent = None
                # reply count of tweet
                if tweet['reply_counts'] is not None and tweet['reply_counts'] != '':
                    replyCount = tweet['reply_counts']
                else:
                    replyCount = None
                #  retweet count
                if tweet['retweet_counts'] is not None and tweet['retweet_counts'] != '':
                    retweetCount = tweet['retweet_counts']
                else:
                    retweetCount = None
                # favourites count of user
                if tweet['author']['favourites_count'] is not None and tweet['author']['favourites_count'] != '':
                    user_favouritesCount = tweet['author']['favourites_count']
                else:
                    user_favouritesCount = None
                # followers count of user
                if tweet['author']['followers_count'] is not None and tweet['author']['followers_count'] != '':
                    user_followersCount = tweet['author']['followers_count']
                else:
                    user_followersCount = None
                # friends count of user
                if tweet['author']['friends_count'] is not None and tweet['author']['friends_count'] != '':
                    user_friendsCount = tweet['author']['friends_count']
                else:
                    user_friendsCount = None
                # user verified
                if tweet['author']['verified'] is not None and tweet['author']['verified'] != '':
                    user_verified = tweet['author']['verified']
                else:
                    user_verified = None
                # user raw description
                if tweet['author']['description'] is not None and tweet['author']['description'] != '':
                    user_rawDescription = tweet['author']['description']
                else:
                    user_rawDescription = None

                # data_dictionary['views'] = data_dictionary.get('views', []) + [views]
                data_dictionary['hashtags'] = data_dictionary.get('hashtags', []) + [hashtags]
                data_dictionary['language'] = data_dictionary.get('language', []) + [language]
                data_dictionary['likeCount'] = data_dictionary.get('likeCount', []) + [likeCount]
                data_dictionary['rawContent'] = data_dictionary.get('rawContent', []) + [rawContent]
                data_dictionary['replyCount'] = data_dictionary.get('replyCount', []) + [replyCount]
                data_dictionary['retweetCount'] = data_dictionary.get('retweetCount', []) + [retweetCount]
                data_dictionary['user_favouritesCount'] = data_dictionary.get('user_favouritesCount', []) + [user_favouritesCount]
                data_dictionary['user_followersCount'] = data_dictionary.get('user_followersCount', []) + [user_followersCount]
                data_dictionary['user_friendsCount'] = data_dictionary.get('user_friendsCount', []) + [user_friendsCount]
                data_dictionary['user_verified'] = data_dictionary.get('user_verified', []) + [user_verified]
                data_dictionary['user_rawDescription'] = data_dictionary.get('user_rawDescription', []) + [user_rawDescription]

def get_all_data_hashtags(directory):
    data_paths = Path(directory)
    data_dictionary = {}
    for _, data_path in tqdm(enumerate(data_paths.glob('*')), total = len(list(data_paths.glob('*')))):
        data = get_data(data_path)
        get_data_classify_hashtags(data, data_dictionary)
    return pd.DataFrame(data_dictionary)