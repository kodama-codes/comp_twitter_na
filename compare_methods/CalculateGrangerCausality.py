import os

import pandas as pd

from compare_methods.TwitterGraphCreator import PartitionType
from compare_methods.GrangerCausality import GrangerCausality

result_csv_path = '../data/comp_results/'
calc_result_path = '../data/gc_results/'

result_data_columns = ['algorithm',
                       'hypothesis',
                       'year',
                       'ftest_lags&p_value',
                       'adf_dist_p_values',
                       'adf_dist_diff_order',
                       'adf_price_p_values',
                       'adf_price_diff_order'
                       ]


def run_adf_test(data, max_order, actual_order, p_values: list = []):
    """
    Recursively run the augmented-dickey-fuller-test and differ the given data if necessary till the test ist either
    successful or max diff order is reached

    :param data: the time series data to test
    :param max_order: max order to difference the data
    :param actual_order: actual order of differencing
    :param p_values: calculated p-values from the ADF-Test
    :return: Test result, the data (maybe in differenced in some order), the actual order and the p_values
    """

    # check if max order is reached
    if actual_order == max_order:
        return False, data, actual_order, p_values

    # run the augmented-dickey-fuller-test
    adf_result, p_value = GrangerCausality.augmented_dicky_fuller_test(data)
    p_values.append(p_value)  # append p-value from test

    if adf_result:  # return if test is true
        return True, data, actual_order, p_values

    else:  # differ the data if and call the method recursively
        # transform data
        data = GrangerCausality.transform_dataframe(data)
        data = data.dropna()  # drop None data after transformation
        return run_adf_test(data, max_order, actual_order + 1, p_values)


if __name__ == '__main__':
    """
    Test for Granger-Causality of the calculated network distances 
    """

    for partition_type in PartitionType:
        for year in ['2018', '2022']:

            # create granger causality result df
            gc_result_df = pd.DataFrame(columns=result_data_columns)

            for root, dirs, files in os.walk(result_csv_path + year + '/'):
                for file in files:

                    if not file.endswith(".csv") or 'statistics' in file or not file.startswith(partition_type.value):
                        continue

                    # read data and set date_time as index
                    result_df = pd.read_csv(root + file, header=0)
                    result_df['date_time'] = pd.to_datetime(result_df['date_time'])
                    result_df.set_index(['date_time'], inplace=True)
                    result_df = result_df.sort_index()
                    result_df = result_df.apply(pd.to_numeric)

                    # for each algorithm test for granger causality and store comp_results
                    for algorithm in result_df.columns.tolist():

                        if algorithm == 'close':  # ignore the btc price column
                            continue

                        # create temporary data frame for the testing
                        temp_result_df = result_df[['close', algorithm]].copy()
                        print(f"Compute GC for {algorithm} with partition: {partition_type} and year {year}")

                        try:

                            # create result dicts for H0 and HA hypothesis
                            result_dict_h0 = {'hypothesis': '$H_0$', 'algorithm': algorithm, 'year': year}
                            result_dict_hA = {'hypothesis': '$H_A$', 'algorithm': algorithm, 'year': year}

                            # ADF-Test for the bitcoin price
                            is_close_stationary, temp_result_df[
                                'close'], adf_price_diff_order, adf_price_p_values = run_adf_test(
                                temp_result_df['close'], max_order=2, actual_order=0, p_values=[])
                            temp_result_df = temp_result_df.dropna()

                            # ADF-Test for the network distances
                            is_dist_stationary, temp_result_df[
                                algorithm], adf_dist_diff_order, adf_dist_p_values = run_adf_test(
                                temp_result_df[algorithm], max_order=2, actual_order=0, p_values=[])
                            temp_result_df = temp_result_df.dropna()

                            # store comp_results of the ADF-Test in result dicts
                            # close
                            result_dict_h0['adf_price_p_values'] = result_dict_hA[
                                'adf_price_p_values'] = adf_price_p_values
                            result_dict_h0['adf_price_diff_order'] = result_dict_hA[
                                'adf_price_diff_order'] = adf_price_diff_order
                            # distance
                            result_dict_h0['adf_dist_p_values'] = result_dict_hA[
                                'adf_dist_p_values'] = adf_dist_p_values
                            result_dict_h0['adf_dist_diff_order'] = result_dict_hA[
                                'adf_dist_diff_order'] = adf_dist_diff_order

                            # if at least one is not stationary the granger causality cannot be calculated
                            if not is_close_stationary or not is_dist_stationary:
                                result_dict_h0['p_value'] = 0.0
                                result_dict_hA['p_value'] = 0.0
                                result_dict_h0['lags&p_value'] = 'Not Stationary Data'
                                result_dict_hA['lags&p_value'] = 'Not Stationary Data'
                                continue

                            # test for granger causality
                            # H0 -> The network distances granger causes the btc close price
                            gc_result_dict_h0 = GrangerCausality.calculate_granger_causality(
                                temp_result_df, algorithm, 'close', max_lag=5)

                            # HA -> btc close price granger causes the network distances
                            gc_result_dict_hA = GrangerCausality.calculate_granger_causality(
                                temp_result_df, 'close', algorithm, max_lag=5)


                        except ValueError as e:
                            result_dict_h0['p_value'] = 0.0
                            result_dict_h0['lags&p_value'] = 'Error encountered'
                            print(e)

                        except Exception as e:
                            print(e)

                        # add the comp_results of the granger causality test to the result dicts
                        result_dict_h0 = GrangerCausality.append_gc_results_to_dict(gc_result_dict_h0, result_dict_h0)
                        result_dict_hA = GrangerCausality.append_gc_results_to_dict(gc_result_dict_hA, result_dict_hA)

                        # add result dicts to the result data frame
                        temp_df = pd.DataFrame([result_dict_h0, result_dict_hA])
                        gc_result_df = pd.concat([gc_result_df, temp_df], ignore_index=True, sort=False)

            # save comp_results to file
            gc_result_file_path = calc_result_path + year + '/' + partition_type.value + "_gc_results.csv"
            gc_result_df.to_csv(gc_result_file_path, index=False)
