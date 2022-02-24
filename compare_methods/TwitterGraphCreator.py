from datetime import datetime, timedelta

import networkx as nx
import pandas as pd

from utils.PartitionType import PartitionType


class TwitterGraphCreator:
    """
    This class creates network graphs from tweets which are getting divided into partition
    """

    PARSE_DATE_TIME_FORMAT: str = '%Y-%m-%d %H:%M:%S'

    # Twitter dataframe converters
    TWITTER_DF_CONVERTERS = {
        'created_at': pd.to_datetime,
        'text': str,
        'hashtags': eval,
        'mentions': eval,
        'domains': eval,
        'user_screen_name': str,
    }

    def __init__(self, tweets_df):
        self.df = tweets_df

    def compute_graphs(self, partition_type: PartitionType):
        """
        Create Graphs from Tweets which are getting divided into partitions
        :param partition_type: partition in which the tweets are getting divided
        :return: list of the created network graphs
        """

        # create partitioned dataframes
        partitioned_dataframe_list = [{'time_stamp': group[0], 'tweets': group[1]} for group in
                                      self.df.resample(partition_type.value)]

        # create partitioned graphs
        partitioned_graph_list = []
        for grouped_tweets in partitioned_dataframe_list:
            partitioned_graph_list.append(
                {'interval_start': self.create_default_date_time(grouped_tweets['time_stamp']),
                 'interval_end': self.create_partition_date_time(grouped_tweets['time_stamp'], partition_type),
                 'partition': partition_type.value,
                 'graph': self.create_twitter_graph(grouped_tweets['tweets'])})
        return partitioned_graph_list

    def create_default_date_time(self, time_stamp):
        """
        Create default formatted date time string
        :param time_stamp: time_stamp
        :return: formatted time string
        """
        return f"{datetime.strftime(time_stamp, self.PARSE_DATE_TIME_FORMAT)}"

    def create_partition_date_time(self, time_stamp, partition_type: PartitionType):
        """
        Create formatted date time string based on given partition
        :param time_stamp: time_stamp
        :param partition_type: partition for which the time string should get created
        :return: formatted time string
        """
        if partition_type is PartitionType.FIVE_MINUTES:
            return f"{datetime.strftime(time_stamp + timedelta(minutes=5), self.PARSE_DATE_TIME_FORMAT)}"
        elif partition_type is PartitionType.FIFTEEN_MINUTES:
            return f"{datetime.strftime(time_stamp + timedelta(minutes=15), self.PARSE_DATE_TIME_FORMAT)}"
        elif partition_type is PartitionType.ONE_HOUR:
            return f"{datetime.strftime(time_stamp + timedelta(hours=1), self.PARSE_DATE_TIME_FORMAT)}"

    @staticmethod
    def create_twitter_graph(df):
        """
        Create a network graph from given twitter data frame
        :param df: data frame containing tweets to generate the network
        :return: Twitter-Graph
        """
        # create new empty Graph
        G = nx.Graph()

        # iterate over rows to add nodes to network
        for idx, row in df.iterrows():

            # add user
            user_node = row['user_screen_name']
            G.add_nodes_from([(user_node, {'type': 'user'})])

            # add user mentions
            for user_mention_node in row['mentions']:

                # handle self mined tweets mentions format
                if not isinstance(user_mention_node, str):
                    user_mention_node = user_mention_node['screen_name']

                # skip self mentions
                if user_mention_node is user_node:
                    continue

                G.add_nodes_from(
                    [(user_mention_node, {'type': 'user'})])
                G.add_edge(user_node, user_mention_node)

            # add hashtags
            for hashtag in row['hashtags']:

                # skip crawled hashtags which the data was crawled for
                if hashtag.lower() in ['btc', 'bitcoin']:
                    continue
                hashtag_node = hashtag.lower()
                G.add_nodes_from([(hashtag_node, {'type': 'hashtag'})])
                G.add_edge(user_node, hashtag_node)

            # add domains
            for domain in row['domains']:
                domain_node = domain.lower()
                G.add_nodes_from([(domain_node, {'type': 'domain'})])
                G.add_edge(user_node, domain_node)

        # remove nodes with 0 degree
        G.remove_nodes_from([n for (n, deg) in G.degree() if deg == 0])

        return G
