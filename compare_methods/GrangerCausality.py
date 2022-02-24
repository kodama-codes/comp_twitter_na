from statsmodels.tsa.stattools import adfuller, grangercausalitytests


class GrangerCausality:

    @staticmethod
    def calculate_granger_causality(df, x_value, y_value, max_lag: int = 1):
        return grangercausalitytests(df[[y_value, x_value]], maxlag=max_lag, verbose=False)

    @staticmethod
    def augmented_dicky_fuller_test(time_series, print_result: bool = False):
        """
        Augmented Dickey-Fuller Test (ADF test)
        ADF test is a popular statistical test for checking whether the Time Series is stationary or not which works
        based on the unit root test. The number of unit roots present in the series indicates the number of
        differencing operations that are required to make it stationary.
        :return: boolean Result, True if stationary, False if not
        """
        result = adfuller(time_series, maxlag=5)
        if print_result:
            print(f'Test Statistics: {result[0]}')
            print(f'p-value: {result[1]}')
            print(f'critical_values: {result[4]}')

        if result[1] > 0.05:
            if print_result:
                print("Series is not stationary")
            return False, round(result[1], 4)
        else:
            if print_result:
                print("Series is stationary")
            return True, round(result[1], 4)

    @staticmethod
    def transform_dataframe(df):
        """
        Difference the given data
        :param df: data to differ
        :return: differenced data
        """
        df_transformed = df.diff().dropna()
        return df_transformed

    @staticmethod
    def append_gc_results_to_dict(gc_results, result_dict):
        """
        Appends the raw calculated comp_results from the granger causality testing to a predefined result dictionary
        :param gc_results: granger causality comp_results
        :param result_dict: predefined result dictionary
        :return: result dictionary containing the gc-comp_results
        """

        # ftest
        ftest_p_value_lag_string = ""
        counter = 1

        for key, value in gc_results.items():
            p_value_results = value[0]

            # ftest comp_results
            ftest_lag_p_value = p_value_results['ssr_ftest'][1]
            formatted_ftest_p_value = "{:5.4f}".format(ftest_lag_p_value)
            ftest_p_value_lag_string = ftest_p_value_lag_string + f"{counter} ({formatted_ftest_p_value}), "
            counter = counter + 1

        # ftest results
        result_dict['ftest_lags&p_value'] = ftest_p_value_lag_string.strip(', ')

        return result_dict
