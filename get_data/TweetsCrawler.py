import datetime
import time
import logging

import tweepy
from pathlib import Path

from utils.MongoDB import MongoDB


class TwitterStreamListener(tweepy.StreamListener):
    """
    A class used to initialize a twitter stream and react to its status updates.
    """

    def __init__(self, end_date_time, mongo_db_collection):
        """
        :param end_date_time: date time when the stream gets terminated
        :param mongo_db_collection: the mongo database collection to store the crawled tweets
        """
        self.end_date_time = end_date_time
        self.mongo_db_collection = mongo_db_collection
        self.log_counter = 0
        super(TwitterStreamListener, self).__init__()

    def on_status(self, tweet):
        """
        This Function gets called everytime the stream receives a Tweet
        """

        # check if end date time of crawling is reached
        if datetime.datetime.now() > self.end_date_time:
            return False  # stream ends
        else:
            if (not tweet.retweeted) and ('RT @' not in tweet.text):  # filter retweets
                if self.log_counter % 5000 == 0:  # log after 5000 tweets
                    logging.info("Crawled {} tweets so far".format(str(self.log_counter)))

                # store the tweet as json object in the mongo db collection
                self.mongo_db_collection.insert_one(document=tweet._json)
                print(tweet._json)
                self.log_counter += 1
            return True  # continue receive tweets

    def on_error(self, status_code):
        """
        This funtion gets called if an error happens during crawling
        :param status_code: the error code which is getting returned form the API
        :return: Fals to stop streaming
        """
        if status_code == 420:  # 420 -> api rate limit reached
            time.sleep(60)
            return False


def initialized_logging():
    """
    Initializes logging.
    Logging directory in root dir of project must exist!
    """
    default_dt_format = "%Y-%m-%d %H:%M:%S"
    ROOT_DIR = str(Path(__file__).absolute().parent.parent)
    file_path = Path(ROOT_DIR + "/logs/", "twitter-crawler").with_suffix(".log")
    logging.basicConfig(format='%(asctime)s %(levelname)s: [%(message)s]', datefmt=default_dt_format,
                        filename=file_path,
                        level=logging.INFO)


def create_date_time(date_time_string):
    """
    Converts String to python date time
    :param date_time_string: date time as string has to be in Format '%Y-%m-%d %H:%M:%S'
    :return: datetime
    """
    return datetime.strptime(date_time_string, '%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    '''
    Starts the twitter crawling process. Please ensure that you have the necessary Twitter API keys and a running 
    MongoDB Instance to connect to.   
    '''

    initialized_logging()
    logging.info("Twitter crawling process started")

    # crawl tweets for 14 days
    start_crawl = create_date_time("2021-12-31 23:59:59")
    stop_crawl = create_date_time("2022-01-16 00:00:01")
    logging.info("Time Range for crawling process set from{} - to {}".format(start_crawl, stop_crawl))

    # define mongo database collection name to store crawled tweets
    mongo_db_collection_name = "someName"

    while datetime.datetime.now() < stop_crawl:  # crawl till end date time is reached
        if datetime.datetime.now() > start_crawl:  # start crawling if when start date time es reached
            try:
                logging.info("Start crawling tweets now")

                # connect to db and create collection
                mongo_db = MongoDB(db_name="TCNA")
                collection = mongo_db.get_create_collection(mongo_db_collection_name)

                # initialize stream
                myStreamListener = TwitterStreamListener(stop_crawl, collection)

                '''
                Connect to twitter streaming API. To obtain the needed API keys please see:
                https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api
                '''
                auth = tweepy.OAuthHandler(TWITTER_TCNA_CONSUMER_KEY, TWITTER_TCNA_CONSUMER_SECRET)
                auth.set_access_token(TWITTER_TCNA_ACCESS_TOKEN, TWITTER_TCNA_ACCESS_TOKEN_SECRET)
                api = tweepy.API(auth)

                # create Stream listener and start crawling
                myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

                # set filter for tweets containing hashtags #btc and/or #bitcoin
                myStream.filter(track=['#btc', '#bitcoin'])

            except Exception as e:
                logging.error("Something went wrong initializing the Crawler and stuff error: {}".format(str(e)))
                time.sleep(60)
        else:
            logging.info("Waiting for the definitive start of crawling, sleep 30s")
            time.sleep(30)

    logging.info("Tweets Crawler finished")
