# -*- coding = utf-8 -*-
# @Time: 2023/12/25 9:19
# @Author: Jiahao Xu
# @File: main_1.12.py
# @Software: PyCharm

import os
import pandas as pd
from datetime import datetime, timedelta

from Connect_Database.connect_database import ConnectDatabase
import price_volume as pv
from factors_quantile import Quantile

start_date = '20170101'
end_date = datetime.now().strftime('%Y%m%d')
data_start_date = datetime.strptime(start_date, '%Y%m%d').date() - timedelta(days=700)
data_start_date = data_start_date.strftime('%Y%m%d')
assets = ['000985.CSI', '000300.SH', '000852.SH', '932000.CSI']


ind_dict = dict()
for asset in assets:
    # index data
    table_name = 'AINDEXEODPRICES'
    columns = 'S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE, S_DQ_VOLUME, S_DQ_AMOUNT'
    condition1 = f"S_INFO_WINDCODE = '{asset}'"
    condition2 = f"TRADE_DT > '{data_start_date}' AND TRADE_DT <= '{end_date}'"
    sql = f''' SELECT %s FROM %s WHERE %s AND %s ''' % (columns, table_name, condition1, condition2)

    cd = ConnectDatabase(sql)
    df1 = cd.get_data()
    df1 = df1.rename(columns={'S_INFO_WINDCODE': 'symbol', 'TRADE_DT': 'date', 'S_DQ_PRECLOSE': 'pre_close',
                              'S_DQ_OPEN': 'open', 'S_DQ_HIGH': 'high', 'S_DQ_LOW': 'low', 'S_DQ_CLOSE': 'close',
                              'S_DQ_VOLUME': 'volume', 'S_DQ_AMOUNT': 'amount'})
    df1[df1.columns[2:]] = (df1[df1.columns[2:]].apply(pd.to_numeric))
    df1 = df1.sort_values(['symbol', 'date']).copy()
    ind_dict[asset] = df1


window1 = 20  # for sigma
window2 = 22  # for Res
window3 = 242  # for kurtosis and skew
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
for index, data1 in ind_dict.items():
    sql = f'''
            SELECT A.S_INFO_WINDCODE, A.TRADE_DT, A.S_DQ_MV, A.FREE_SHARES_TODAY
            FROM ASHAREEODDERIVATIVEINDICATOR A
            WHERE A.TRADE_DT > '{data_start_date}' AND A.S_INFO_WINDCODE IN (
                SELECT B.S_CON_WINDCODE
                FROM AINDEXMEMBERS B
                WHERE B.S_INFO_WINDCODE = '{index}'
                    AND (
                        (B.S_CON_INDATE <= A.TRADE_DT AND B.S_CON_OUTDATE > A.TRADE_DT)
                    OR (B.S_CON_INDATE <= A.TRADE_DT AND B.S_CON_OUTDATE IS NULL)
                    )
                )
            '''
    cd = ConnectDatabase(sql)
    df2 = cd.get_data()
    df2 = df2.rename(columns={'S_INFO_WINDCODE': 'symbol', 'TRADE_DT': 'date', 'S_DQ_MV': 'cap',
                     'FREE_SHARES_TODAY': 'free_share'})
    df2[df2.columns[2:]] = df2[df2.columns[2:]].apply(pd.to_numeric, errors='coerce')
    df2.sort_values(['symbol', 'date'], inplace=True)
    temp1 = pv.fluidity_factors(data1, stk_df=df2)
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
for index, data1 in ind_dict.items():
    temp2 = pv.volatility_factors(data1, window1)
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
for index, data1 in ind_dict.items():
    temp3 = pv.Res(data1, window2)
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
for index, data1 in ind_dict.items():
    temp4 = pv.high_moment(data1, window3)
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


for index, data1 in ind_dict.items():
    temp5 = pv.volumes(data1)
    temp_path_1 = path_1 + '/volume'
    if not os.path.exists(temp_path_1):
        os.mkdir(temp_path_1)
    temp5.to_parquet(temp_path_1 + f'/ind_{index}_{start_date}_{end_date}.parquet')

for index, data1 in ind_dict.items():
    temp2 = pv.volatility_factors(data1, window1)
    temp6 = pv.sigma_return_corr(data1, temp2)
    temp_path = path_3 + '/sigma_ret_corr'
    if not os.path.exists(temp_path):
        os.mkdir(temp_path)
    temp6.to_parquet(temp_path + f'/ind_{index}_{start_date}_{end_date}.parquet')

for index, data1 in ind_dict.items():
    temp7 = pv.momentum(data1)
    temp_path = path_4
    if not os.path.exists(temp_path):
        os.mkdir(temp_path)
    temp7.to_parquet(temp_path + f'/ind_{index}_{start_date}_{end_date}.parquet')