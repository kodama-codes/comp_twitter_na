from datetime import datetime, timedelta
import requests

import pandas as pd


class TweetParser:
    """
    Prepares the raw crawled tweets"
    """
    twitter_date_time_format = '%a %b %d %H:%M:%S +0000 %Y'

    result_df_columns = ['id',
                         'created_at',
                         'text',
                         'hashtags',
                         'mentions',
                         'domains',
                         'user_screen_name']

    def __init__(self, mongo_collection, date_time_start: datetime, date_time_end: datetime,
                 resolve_tco_urls: bool = True, allow_retweets: bool = False):

        self.mongo_collection = mongo_collection
        self.time_range_start = date_time_start.date()
        self.time_range_end = date_time_end.date()
        self.resolve_tco_urls = resolve_tco_urls
        self.allow_retweets = allow_retweets
        self.processed_tweets_count = 0

    def prepare_crawled_tweets(self):
        """
        Iterates over the crawled tweets in the given mongo database collection and prepares the data.
        :return: the prepared data as dataframe
        """

        # log
        print(f"Start parsing raw tweets data for {self.mongo_collection.name}")

        # create empty result dataframe
        result_df = pd.DataFrame(columns=self.result_df_columns)

        # create empty list to temporary save process tweets as dicts
        temp_tweet_dict_list = []

        # iterate over tweets in collection
        for tweet in self.mongo_collection.find():

            # check for retweets
            if not self.allow_retweets and tweet['retweeted']:
                continue

            # ---------------- id ----------------
            # create tweet with id
            tweet_dict = {'id': tweet['id']}

            # ---------------- created_at ----------------
            tweet_datetime = datetime.strptime(tweet['created_at'], self.twitter_date_time_format)

            # skip tweet if is not in defined date time range
            if not self.is_in_time_range(tweet_datetime.date()):
                continue

            # format date time as desired
            tweet_dict['created_at'] = datetime.strftime(tweet_datetime, '%Y-%m-%d %H:%M:%S')

            # ---------------- entities ----------------
            if 'extended_tweet' not in tweet:
                entities_data = tweet['entities']
                tweet_dict['text'] = tweet['text']  # maybe utf-8 encoding needed ?
            else:
                extended_data = tweet['extended_tweet']
                tweet_dict['text'] = extended_data['full_text']  # maybe utf-8 encoding needed ?
                entities_data = extended_data['entities']

            tweet_dict['hashtags'] = [h['text'] for h in entities_data['hashtags']]
            tweet_dict['mentions'] = [{'id': m['id'], 'screen_name': m['screen_name']}
                                      for m in
                                      entities_data['user_mentions']]

            # ---------------- entities ----------------
            tweet_dict['domains'] = [d['url'] for d in entities_data['urls']]
            if self.resolve_tco_urls:
                # as Twitter only stores the tco urls as entities in the raw tweets, they have to be resolved first
                tweet_dict['domains'] = self.resolve_tco_url_list(tweet_dict['domains'])

            # ---------------- user screen name ----------------
            user = tweet['user']
            tweet_dict['user_screen_name'] = user['screen_name']

            # append dict to list
            temp_tweet_dict_list.append(tweet_dict)
            self.processed_tweets_count += 1  # increase counter

            # append to df all 2000 tweets to get best performance
            if self.processed_tweets_count % 2000 == 0 or ():
                # append to df
                result_df = result_df.append(temp_tweet_dict_list, ignore_index=True, sort=False)
                # reset tweet_dict
                temp_tweet_dict_list = []
                # print
                print(f"Prepared tweets: {str(self.processed_tweets_count)}")

        # append the last tweets
        if len(tweet_dict) != 0:
            result_df = result_df.append(temp_tweet_dict_list, ignore_index=True, sort=False)

        # set created at column to index
        result_df.set_index('created_at', inplace=True)

        # finally return the dataframe
        return result_df

    def is_in_time_range(self, x):
        """Return true if x is in the range [start, end]"""
        if self.time_range_start <= self.time_range_end:
            return self.time_range_start <= x <= self.time_range_end
        else:
            return self.time_range_start <= x or x <= self.time_range_end

    @staticmethod
    def resolve_tco_url_list(domains):
        """
        Resolve a given list of domains
        :param domains: domains to resolve e.g. tco domains
        :return: list of resolved domains
        """
        if len(domains) <= 0:
            return []
        else:
            return [TweetParser.get_domain_from_tco_url(url) for url in domains]

    @staticmethod
    def get_domain_from_tco_url(url):
        """
        Resolve a single given url
        :param url: url to resolve e.g. a tco url
        :return: resolved url
        """
        try:
            r = requests.get(url, timeout=3)  # resolve tco.url
            url = r.url.replace("https://", "")
            url = url.replace("http://", "")
            plain_url = url.split("/")[0]
            if plain_url.startswith("www."):
                plain_url = plain_url.replace("www.", "")
            return plain_url
        except:
            return
