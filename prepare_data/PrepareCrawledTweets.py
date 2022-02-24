from datetime import datetime, timedelta


from prepare_data.TweetParser import TweetParser
from utils.MongoDB import MongoDB


def create_date_time(date_time_string):
    """
    Converts String to python date time
    :param date_time_string: date time as string has to be in Format '%Y-%m-%d %H:%M:%S'
    :return: datetime
    """
    return datetime.strptime(date_time_string, '%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    ################################################ configuration #####################################################

    # initialize MongoDB connection and fetch collection
    db = MongoDB(db_name='TCNA')
    tweet_collection = db.get_create_collection("tweets_2022")
    dest_path = '../data/tweets/2022/'
    file_name = '2022_01-1.csv'
    # define start and end date and time to ensure only tweets in this range are getting processed
    date_time_start = create_date_time("2022-01-01 00:00:00")
    date_time_end = create_date_time("2022-01-15 23:59:59")

    # the utc time delta is used as the system time is created including utc and the crawled tweets have utc=0
    utc_hour_delta = 1  # Summer +2 / Winter + 1

    ####################################################################################################################
    utc_zero_date_time_start = date_time_start - timedelta(hours=utc_hour_delta)
    utc_zero_date_time_end = date_time_end - timedelta(hours=utc_hour_delta)

    # initialize TweetParser
    parser = TweetParser(mongo_collection=tweet_collection, date_time_start=utc_zero_date_time_start,
                         date_time_end=utc_zero_date_time_end, resolve_tco_urls=True, allow_retweets=False)

    # parse and save tweets
    data_df = parser.get_parsed_tweets(save_to_csv=True)
    data_df.to_csv(dest_path + file_name)
