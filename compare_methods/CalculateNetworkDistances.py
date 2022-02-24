import os
import time

from compare_methods.BTCPriceDataCreator import BTCPriceDataCreator
from compare_methods.TwitterGraphComparator import TwitterGraphComparator
from compare_methods.TwitterGraphCreator import *

from utils.PartitionType import PartitionType


def read_monthly_data(tweets_data_path, year):
    """
    Read the monthly tweets data csv Files and create one full data frame
    :param tweets_data_path: the path where the files are
    :param year: the year in which the data is collected
    :return: complete data frame containing all tweets
    """
    print(f"Start reading Files for Method Comparison for year {year}")
    # Twitter dataframe converters
    twitter_df_converters = {
        'created_at': pd.to_datetime,
        'text': str,
        'hashtags': eval,
        'mentions': eval,
        'domains': eval,
        'user_screen_name': str,
    }

    # create time frame based graphs
    monthly_df_list = []
    for root, dirs, files in os.walk(tweets_data_path + year + '/'):
        for file in files:

            if not file.endswith(".csv"):
                continue
            start_time = time.time()
            monthly_df = pd.read_csv(root + file, converters=twitter_df_converters, header=0)
            monthly_df.set_index('created_at', inplace=True)
            monthly_df_list.append(monthly_df)
            print(
                f"Successfully loaded data from file: {file} with shape {monthly_df.shape[0]} took: "
                f"{round(float(time.time() - start_time), 2)}[s]")

    complete_tweets_df = pd.concat(monthly_df_list)
    print(f"Concatenated all monthly tweets final size: {complete_tweets_df.shape[0]}")
    return complete_tweets_df


def calc_mean_duration_time(time_graph_data_df):
    """
    Calculate the mean duration of the graph comparisons
    :param time_graph_data_df: the graph data
    :return: mean duration time
    """
    return timedelta(seconds=round(time_graph_data_df['duration'].mean(), 5))


def calc_mean_node_size(node_graph_data_df):
    """
    Calculate the mean node size of the graphs
    :param node_graph_data_df: the graph data
    :return: mean node size
    """
    return round((node_graph_data_df['g1_node_size'].mean() + node_graph_data_df['g2_node_size'].mean()) / 2, 0).astype(
        int)


def create_and_save_statistics(year, dest_file_path, comp_results):
    """
    Create and save statistics of the graph comparison
    :param year: the year in which the data is collected
    :param dest_file_path: destination file path
    :param comp_results: the
    """
    algorithms = [comp_result['algorithm'] for comp_result in comp_results]
    mean_duration_times = [calc_mean_duration_time(comp_result['data']) for comp_result in
                           compare_results]
    mean_node_sizes = [calc_mean_node_size(comp_result['data']) for comp_result in
                       compare_results]
    statistics_result_df = pd.DataFrame.from_dict(
        {'algorithm': algorithms, 'mean_duration_time': mean_duration_times, 'mean_node_size': mean_node_sizes})
    statistics_result_path = dest_file_path + year + '/' + partition_type.value + "_statistics.csv"
    statistics_result_df.to_csv(statistics_result_path, index=False)


if __name__ == '__main__':
    """
    Calculate the network distances based on the chosen network comparison methods.
    """

    # read in Tweets
    tweets_data_path = '../data/tweets/'
    # comp_results path
    result_csv_path = '../data/comp_results/'

    # calculate distances for the data of the years 2018 and 2022
    for year in ['2018', '2022']:

        # read tweets data
        tweets_df = read_monthly_data(tweets_data_path, year)

        for partition_type in PartitionType:

            print(f"############## Start processing Partition Type {partition_type.value} ##############")

            # create a list of graphs for the given partition_type
            print(f"Create partitioned Graph list for partition type: {partition_type.value} and year {year}")
            graph_creator = TwitterGraphCreator(tweets_df)
            graph_list = graph_creator.compute_graphs(partition_type)

            # calculate the graph distances based on the used comparison methods
            print(f"Calculate network distances for partition type: {partition_type.value} and year {year}")
            twitter_graph_comparator = TwitterGraphComparator(graph_list)
            compare_results = twitter_graph_comparator.compute_graph_distances(normalized=True)

            # fetch the bitcoin price data
            print(f"Fetch bitcoin price data for year {year}")
            btc_price_data_creator = BTCPriceDataCreator(year, partition_type)
            btc_price_data = btc_price_data_creator.get_prepared_price_df()

            # create and save statistic data of distance computation
            print(f"Create and save statistics for partition type: {partition_type.value} and year {year}")
            create_and_save_statistics(year, result_csv_path, compare_results)

            # merge the calculated network distances into one data frame
            for count, compare_result in enumerate(compare_results):
                # prepare data -> df {'algorithm' : 'MCS', 'data' : data_df}
                graph_data_df = compare_result['data']

                # merge graph data with btc price data
                graph_data_df.set_index(pd.to_datetime(graph_data_df['date_time']), inplace=True)

                # rename algorithm name
                graph_data_df = graph_data_df.rename(columns={'distance': compare_result['algorithm']})
                graph_data_df = graph_data_df[[compare_result['algorithm']]]

                if count == 0:
                    merged_data_df = pd.merge(btc_price_data, graph_data_df, how='inner', left_index=True,
                                              right_index=True)
                else:
                    merged_data_df = pd.merge(merged_data_df, graph_data_df, how='inner', left_index=True,
                                              right_index=True)

            # store timeseries of btc price data and network distances to csv file
            merged_data_file_path = result_csv_path + year + '/' + partition_type.value + "_comp_data.csv"
            merged_data_df.to_csv(merged_data_file_path)
