from datetime import datetime, timedelta
import json

import requests
import pandas as pd

from utils.PartitionType import PartitionType


def get_seconds_for_partition_type(partition_type):
    """
    Converts a give partition type to its corresponding amount of seconds
    :param partition_type: partition type to transform
    :return: seconds:int
    """
    if partition_type == PartitionType.FIVE_MINUTES:
        return 60 * 5
    if partition_type == PartitionType.FIFTEEN_MINUTES:
        return 60 * 15
    if partition_type == PartitionType.ONE_HOUR:
        return 60 * 60


def fetch_data(symbol_pair, start_date_time, end_date_time, granularity):
    """
    Fetch historical price date from the coinbase api.

    :param symbol_pair: the symbol pair to fetch e.g. BTC-USD
    :param start_date_time: start date time for which teh data is getting fetched
    :param end_date_time: end date time for which the data is getting fetched
    :param granularity: the granularity in which the data is getting fetched
    :return: fetched price data
    """
    # create result dataframe
    price_df = pd.DataFrame(columns=['unix', 'low', 'high', 'open', 'close', 'volume'])

    coinbase_api_base_url = "https://api.pro.coinbase.com/"
    hist_data_api_endpoint = f"products/{symbol_pair}/candles?start={start_date_time}&end={end_date_time}&" \
                             f"granularity={granularity}"

    # fetch data
    response = requests.get(coinbase_api_base_url + hist_data_api_endpoint)

    if response.status_code == 200:  # check response code to make sure the response from server is ok

        data = json.loads(response.text)  # create json from response

        # if we failed to get any data, print an error...otherwise write the file
        if data is None:
            print("Did not return any data from Coinbase for this symbol")
            return None
        else:
            for d in data:
                price_df.loc[len(price_df)] = d
            price_df['date'] = pd.to_datetime(price_df['unix'], unit='s')  # convert to readable date
            return price_df
    else:
        print("Did not receive OK response from Coinbase API")
        return None


def create_date_time(date_time_string):
    """
    Converts String to python date time
    :param date_time_string: date time as string has to be in Format '%Y-%m-%d %H:%M:%S'
    :return: datetime
    """
    return datetime.strptime(date_time_string, '%Y-%m-%d %H:%M:%S')


if __name__ == "__main__":
    '''
    Fetching historical bitcoin price data with coinbase api.
    '''
    ################################################ configuration #####################################################

    result_file_path = "../data/btc_price_data/"  # path where the file with the fetched prices ist getting stored

    # start and end time where the data is getting fetched
    datetime_start = create_date_time("2022-01-01 00:00:00")
    datetime_end = create_date_time("2022-01-15 23:59:59")

    # symbol pair e.g. cryptocurrency and fiat currency
    symbol_pair = "BTC-USD"
    ####################################################################################################################

    coinbase_api_date_time_format = "%Y-%m-%dT%H:%M:%S"  # start -> 2021-07-17T23:59:59

    # fetch bitcoin prices for every partition type
    for partition_type in PartitionType:

        print(
            f"Start fetching data for symbol pair {symbol_pair} for partition type {partition_type} from "
            f"{datetime_start} to {datetime_end}")

        # set start and end time to fetch
        fetch_start_date_time = datetime_start
        fetch_end_date_time = fetch_start_date_time + timedelta(hours=1) - timedelta(seconds=1)

        # also write header on writing first data
        write_header = True

        granularity = get_seconds_for_partition_type(partition_type)

        file_name = f"{symbol_pair}_" + partition_type.value + ".csv"

        # start fetching data
        while True:

            # fetch data
            data_df = fetch_data(
                symbol_pair=symbol_pair, start_date_time=fetch_start_date_time.strftime(coinbase_api_date_time_format),
                end_date_time=fetch_end_date_time.strftime(coinbase_api_date_time_format), granularity=granularity)

            # if data is successfully fetched write data to file in append mode
            if data_df is not None:

                data_df = data_df.sort_values('date', ascending=True)
                data_df.to_csv(result_file_path + file_name, mode='a',
                               index=False, header=write_header)

                # after writing first entry set write header to false
                write_header = False

                print(f"{file_name} - fetched data for start:{fetch_start_date_time} end:{fetch_end_date_time}")

                # set new start and end time
                fetch_start_date_time = fetch_end_date_time + timedelta(seconds=1)
                fetch_end_date_time = fetch_start_date_time + timedelta(hours=1) - timedelta(seconds=1)
                if fetch_end_date_time > datetime_end:
                    break
            else:
                break
