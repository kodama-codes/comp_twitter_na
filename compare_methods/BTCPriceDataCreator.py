import pandas as pd


class BTCPriceDataCreator:
    """
    Provides the bitcoin price data for a given year and partition type
    """

    data_path = '../data/btc_price_data/'

    def __init__(self, year, partition_type):
        filename = f"{year}_{partition_type.value}.csv"
        self.btc_data = pd.read_csv(self.data_path + filename, header=0, converters={'date': pd.to_datetime})
        self.btc_data = self.btc_data.rename(columns={'date': 'date_time'})
        self.btc_data = self.btc_data[['date_time', 'close']]
        self.btc_data.set_index(pd.to_datetime(self.btc_data['date_time']), inplace=True)
        self.btc_data = self.btc_data[['close']]

    def get_prepared_price_df(self):
        """
        Get prepared bitcoin price data frame
        :return: BTC data frame
        """
        return self.btc_data
