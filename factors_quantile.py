# -*- coding = utf-8 -*-
# @Time: 2023/12/25 9:19
# @Author: Jiahao Xu
# @File: factors_quantile.py
# @Software: PyCharm

import pandas as pd
from bisect import bisect_left


class Quantile:
    """ """
    def __init__(self, data, factor_list):
        """ """
        self.data = data
        self.factor_list = factor_list

    def formula_q(self, numbers):
        """ """
        if pd.isna(numbers):
            return None
        else:
            values = list(range(100))
            q_values = min(values, key=lambda x: abs(x - numbers))
            return q_values

    # def percentile_rank(self, window_data):
    #    return (bisect_left(window_data.sort_values(), window_data[-1]) + 1) / len(window_data) * 100

    def transform_to_q(self):
        for factor in self.factor_list:
            factor_ranks = self.data[factor].rolling(242).rank()/242 * 100
            self.data[factor + '_Q'] = factor_ranks.apply(lambda x: self.formula_q(x) if not pd.isna(x) else None)
        return self.data
