import os
import re

import pandas as pd


def get_base_domain_from_url(url):
    """
    Extracts the base domain of a plain url
    :param url: url
    :return: base domain of the given url
    """
    if url:
        try:
            url = url.replace("https://", "")
            url = url.replace("http://", "")
            plain_url = url.split("/")[0]
            if plain_url.startswith("www."):
                plain_url = plain_url.replace("www.", "")
            return plain_url
        except:
            return
    else:
        return url


if __name__ == '__main__':
    """
    Start preparing the raw data consisting of the tweets from kaggle.
    https://www.kaggle.com/jaimebadiola/177-million-bitcoin-tweets
    """
    ################################################ configuration #####################################################

    source_path = '../data/raw/2018/'
    dest_path = '../data/prepared/2018/'

    ####################################################################################################################

    # define converters for reading data
    data_converters_map = {
        'hashtags': str,
        'mentions': str,
    }

    for root, monthly_dirs, root_files in os.walk(source_path):

        # iterate over monthly directories
        for month_dir in monthly_dirs:

            daily_df_list = []  # create list to store the prepared daily data frames

            # iterate over files per month
            for sub_root, dirs, files in os.walk(source_path + month_dir + "/"):
                for file in files:
                    if file.endswith(".csv"):
                        print(f"Start preparing daily file: {file}")

                        # read csv file
                        df = pd.read_csv(sub_root + file, sep=";", error_bad_lines=False, header=0,
                                         converters=data_converters_map)

                        # rename the username column
                        df = df.rename(columns={'username': 'user_screen_name'})

                        # parse date time and set as index
                        df['created_at'] = pd.to_datetime(df['date'])
                        df.set_index('created_at', inplace=True)

                        # parse hashtags to list without #simbyol e.g. ['Cryptocurrency', 'crypto', 'bitcoin']
                        df['hashtags'] = [[s for s in l if s] for l in df['hashtags'].str.split('#')]

                        # cast mentions to list with user screen names (we don't use ID anymore as usernames are unique)
                        df['mentions'] = [[s for s in l if s] for l in df['mentions'].str.split('@')]

                        # extract links from text and only keep domain
                        pattern = r'(https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._+~#=]{1,256}\.[a-zA-Z0-9()]{1,' \
                                  r'6}[-a-zA-Z0-9()@:%_+.~#?&/=]*) '
                        df['domains'] = df.text.apply(lambda x: re.findall(pattern, str(x)))
                        df['domains'] = [[get_base_domain_from_url(url) for url in urls if url] for urls in
                                         df['domains']]

                        # drop rows where id is nan
                        df.dropna(subset=['id'])

                        # drop rows with duplicate id
                        df = df.drop_duplicates('id')

                        # drop unnamed rows
                        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

                        # select only necessary rows to keep file size small
                        df = df[['id', 'user_screen_name', 'text', 'hashtags', 'mentions', 'domains']]

                        # append to monthly df
                        daily_df_list.append(df)

                # concat the daily dataframes to one complete monthly dataframe
                final_df = pd.concat(daily_df_list, axis=0)
                final_df = final_df.sort_index()
                print(f"End prepare month: {month_dir} - df_size: {final_df.shape[0]}")
                # save data to csv file
                final_df.to_csv(dest_path + month_dir + ".csv")
