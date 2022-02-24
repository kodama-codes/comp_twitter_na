import gmatch4py as gm
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

from utils.StopWatch import StopWatch


class TwitterGraphComparator:
    """
    Compares a given list of graphs and calculates the distance between them. A pre chosen set of algorithms are used
    to calculate the network distances.
    """

    # pre chosen algorithms to calculate the network distances
    graph_matching_algorithms = [
        # -- Graph Edit Distance -- #
        # gm.GreedyEditDistance, -> commented because this method takes way to loong
        gm.MCS,
        # -- Iterative Methods -- #
        gm.Jaccard,
        gm.VertexRanking,
        gm.VertexEdgeOverlap,
        gm.BagOfCliques,
        gm.BagOfNodes,
        # -- Graph Kernels -- #
        gm.WeisfeleirLehmanKernel
    ]

    def __init__(self, graphs_to_compare):
        self.graphs_to_compare = graphs_to_compare

    def compute_graph_distances(self, normalized: bool = True):
        """
        Calculates the distances between the given networks.
        :param normalized: Normalize calculated distances
        :return: Calculated distances data frame
        """

        # initialize the stop watch
        stop_watch = StopWatch()

        # initialize result data frame list
        result_df_list = []

        # iterate over predefined algorithms to calculate the network distances
        for algorithm in self.graph_matching_algorithms:

            # initialize algorithm
            print(f"Start computation for {algorithm.__name__}")
            comp_algorithm = self.initialize_graph_matching_algorithm(algorithm)

            # create result data frame
            column_names = ["g1_interval", "g2_interval", "g1_node_size", "g2_node_size", "duration", "distance"]
            result_df = pd.DataFrame(columns=column_names)
            counter = 0
            graphs_to_compare_size = len(self.graphs_to_compare)
            result_dict_list = []

            # iterate over the networks to compare them pairwise.
            # list of Graphs [A,B,C,D] is getting compared like A-B, B-C, C-D
            for idx in range(1, graphs_to_compare_size):

                result_dict = {}
                data_1 = self.graphs_to_compare[idx - 1]
                data_2 = self.graphs_to_compare[idx]
                g1 = data_1['graph']
                g2 = data_2['graph']
                result_dict['g1_interval'] = data_1['interval_start']
                result_dict['g2_interval'] = data_2['interval_start']
                result_dict['date_time'] = data_2['interval_end']
                result_dict['g1_node_size'] = g1.number_of_nodes()
                result_dict['g2_node_size'] = g2.number_of_nodes()

                # start stopwatch
                stop_watch.start()

                # calculate the distance between the graphs based on the used algorithm
                if g1.number_of_nodes() <= 0 and g2.number_of_nodes() <= 0:
                    result_dict['distance'] = 0.0
                elif algorithm == gm.WeisfeleirLehmanKernel:
                    result_dict['distance'] = \
                        np.asarray(comp_algorithm.distance(comp_algorithm.compare([g1, g2], None)))[0][1]
                else:
                    result_dict['distance'] = comp_algorithm.distance(comp_algorithm.compare([g1, g2], None))[0][1]

                # stop stopwatch
                result_dict['duration'] = stop_watch.get_time()
                stop_watch.reset()

                # add result dict to list
                result_dict_list.append(result_dict)
                counter = counter + 1

                # after 5000 comp_results add the result dicts to data frame for better performance
                if counter % 5000 == 0:
                    print(f"{algorithm.__name__} - compared {counter} graphs of {graphs_to_compare_size}")
                    temp_df = pd.DataFrame.from_records(result_dict_list)
                    result_df = pd.concat([result_df, temp_df])
                    result_dict_list = []

            # add rest of calculated distances
            temp_df = pd.DataFrame.from_records(result_dict_list)
            result_df = pd.concat([result_df, temp_df])

            # normalize distance
            if normalized:
                min_max_scaler = MinMaxScaler()
                result_df[['distance']] = min_max_scaler.fit_transform(result_df[['distance']])

            # round distance to 5 digits
            result_df['distance'] = result_df['distance'].round(5)

            # append to result list
            result_df_list.append({'algorithm': algorithm.__name__, 'data': result_df})

        return result_df_list

    @staticmethod
    def initialize_graph_matching_algorithm(algorithm):
        """
        Initialize an instance for the given algorithm
        :param algorithm: network comparison algorithm
        :return: Instance of the given Algorithm
        """
        if algorithm in (gm.GraphEditDistance, gm.BP_2, gm.GreedyEditDistance, gm.HED):
            return algorithm(1, 1, 1, 1)
        elif algorithm == gm.WeisfeleirLehmanKernel:
            return algorithm(h=1)
        else:
            return algorithm()
