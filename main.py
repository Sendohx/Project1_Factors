# -*- coding = utf-8 -*-

import os
import pandas as pd
from datetime import datetime, timedelta

import price_volume as pv
from factors_quantile import Quantile

# data parameters
start_date = '20170101'
end_date = datetime.now().strftime('%Y%m%d')
data_start_date = datetime.strptime(start_date, '%Y%m%d').date() - timedelta(days=700)
data_start_date = data_start_date.strftime('%Y%m%d')
assets = ['000985.CSI', '000300.SH', '000852.SH', '932000.CSI']

ind_dict = dict()
for asset in assets:
    df = pd.read_parquet(f'/nas92/xujiahao/data/raw/ind_{asset}_{start_date}_{end_date}.parquet')
    df = df.sort_values(['symbol', 'date']).copy()
    ind_dict[asset] = df

# factor parameters
window1 = 20  # for sigma
window2 = 22  # for Res
window3 = 242  # for kurtosis and skew

# 创建因子落库路径
path = f'/nas92/factor/day_frequency/volume_price'
path_1 = path + f'/fluidity'
if not os.path.exists(path_1):
    os.mkdir(path_1)
path_2 = path + f'/volatility'
if not os.path.exists(path_2):
    os.mkdir(path_2)
path_3 = path + f'/high_moment'
if not os.path.exists(path_3):
    os.mkdir(path_3)
path_4 = path + f'/momentum'
if not os.path.exists(path_4):
    os.mkdir(path_4)

# 流动性因子
for index, ind_data in ind_dict.items():
    stk_data = pd.read_parquet(f'/nas92/xujiahao/data/raw/stk_{index}_{start_date}_{end_date}.parquet')
    temp1 = pv.fluidity_factors(ind_data, stk_df=stk_data)
    temp_list1 = temp1.columns[2:]
    Q1 = Quantile(temp1, temp_list1)
    result1 = Q1.transform_to_q()
    result1 = result1[result1['date'] >= start_date]
    for factor in temp_list1:
        temp_path_1 = path_1 + f'/{factor}'
        if not os.path.exists(temp_path_1):
            os.mkdir(temp_path_1)
            result1[['date', factor, factor + '_Q']].to_parquet(
                temp_path_1 + f'/ind_{index}_{start_date}_{end_date}.parquet')
        else:
            result1[['date', factor, factor + '_Q']].to_parquet(
                temp_path_1 + f'/ind_{index}_{start_date}_{end_date}.parquet')

# 波动性因子
for index, ind_data in ind_dict.items():
    temp2 = pv.volatility_factors(ind_data, window1)
    temp_list2 = temp2.columns[2:]
    Q2 = Quantile(temp2, temp_list2)
    result2 = Q2.transform_to_q()
    result2 = result2[result2['date'] >= start_date]
    for factor in temp_list2:
        temp_path_1 = path_2 + f'/{factor}'
        temp_path_2 = temp_path_1 + f'/window={window1}'
        if not os.path.exists(temp_path_1):
            os.mkdir(temp_path_1)
            os.mkdir(temp_path_2)
            result2[['date', factor, factor + '_Q']].to_parquet(
                temp_path_2 + f'/ind_{index}_{start_date}_{end_date}.parquet')
        else:
            result2[['date', factor, factor + '_Q']].to_parquet(
                temp_path_2 + f'/ind_{index}_{start_date}_{end_date}.parquet')

# 弹性因子
for index, ind_data in ind_dict.items():
    temp3 = pv.Res(ind_data, window2)
    temp_list3 = temp3.columns[2:]
    Q3 = Quantile(temp3, temp_list3)
    result3 = Q3.transform_to_q()
    result3 = result3[result3['date'] >= start_date]
    for factor in temp_list3:
        temp_path_1 = path_1 + f'/{factor}'
        temp_path_2 = temp_path_1 + f'/window={window2}'
        if not os.path.exists(temp_path_1):
            os.mkdir(temp_path_1)
            os.mkdir(temp_path_2)
            result3[['date', factor, factor + '_Q']].to_parquet(
                temp_path_2 + f'/ind_{index}_{start_date}_{end_date}.parquet')
        else:
            result3[['date', factor, factor + '_Q']].to_parquet(
                temp_path_2 + f'/ind_{index}_{start_date}_{end_date}.parquet')

# 高阶矩因子
for index, ind_data in ind_dict.items():
    temp4 = pv.high_moment(ind_data, window3)
    temp_list4 = temp4.columns[2:]
    Q4 = Quantile(temp4, temp_list4)
    result4 = Q4.transform_to_q()
    result4 = result4[result4['date'] >= start_date]
    for factor in temp_list4:
        temp_path_1 = path_3 + f'/{factor}'
        temp_path_2 = temp_path_1 + f'/window={window3}'
        if not os.path.exists(temp_path_1):
            os.mkdir(temp_path_1)
            os.mkdir(temp_path_2)
            result4[['date', factor, factor + '_Q']].to_parquet(
                temp_path_2 + f'/ind_{index}_{start_date}_{end_date}.parquet')
        else:
            result4[['date', factor, factor + '_Q']].to_parquet(
                temp_path_2 + f'/ind_{index}_{start_date}_{end_date}.parquet')

# 成交量
for index, ind_data in ind_dict.items():
    temp5 = pv.volumes(ind_data)
    temp_path_1 = path_1 + '/volume'
    if not os.path.exists(temp_path_1):
        os.mkdir(temp_path_1)
    temp5.to_parquet(temp_path_1 + f'/ind_{index}_{start_date}_{end_date}.parquet')

# sigma和收益率相关系数
for index, ind_data in ind_dict.items():
    temp2 = pv.volatility_factors(ind_data, window1)
    temp6 = pv.sigma_return_corr(ind_data, temp2)
    temp_path = path_3 + '/sigma_ret_corr'
    if not os.path.exists(temp_path):
        os.mkdir(temp_path)
    temp6.to_parquet(temp_path + f'/ind_{index}_{start_date}_{end_date}.parquet')

# 动量
for index, ind_data in ind_dict.items():
    temp7 = pv.momentum(ind_data)
    temp_path = path_4
    if not os.path.exists(temp_path):
        os.mkdir(temp_path)
    temp7.to_parquet(temp_path + f'/ind_{index}_{start_date}_{end_date}.parquet')
