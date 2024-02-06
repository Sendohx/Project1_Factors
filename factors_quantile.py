# -*- coding = utf-8 -*-

import pandas as pd


class Quantile:
    """计算历史分位值"""
    def __init__(self, data, factor_list):
        """
        :param data: 数据
        :param factor_list: 需要计算分位值的因子列表
        """
        self.data = data
        self.factor_list = factor_list

    def formula_q(self, numbers):
        """
        :param numbers: 用于计算分位值的序列
        """
        if pd.isna(numbers):
            return None
        else:
            values = list(range(100))
            q_values = min(values, key=lambda x: abs(x - numbers))
            return q_values

    # def percentile_rank(self, window_data):
    #    return (bisect_left(window_data.sort_values(), window_data[-1]) + 1) / len(window_data) * 100

    def transform_to_q(self):
        """计算历史分位值"""
        for factor in self.factor_list:
            factor_ranks = self.data[factor].rolling(242).rank()/242 * 100
            self.data[factor + '_Q'] = factor_ranks.apply(lambda x: self.formula_q(x) if not pd.isna(x) else None)
        return self.data
