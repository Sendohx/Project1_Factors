# -*- coding = utf-8 -*-
# @Time: 2023/12/25 9:19
# @Author: Jiahao Xu
# @File: price_volume.py
# @Software: PyCharm

import numpy as np
from scipy.stats import kurtosis, skew

def fluidity_factors(ind_df, stk_df=None):
    """"""
    df = ind_df.sort_values('date').copy()
    df['return'] = df['close']/df['pre_close'] - 1
    df.loc[:, 'ILLIQ'] = df['return'].abs()*100 / (df['amount']/100000000)
    df.loc[:, 'ABILLIQ'] = df['ILLIQ'] - df['ILLIQ'].rolling(120).mean()
    df.loc[:, 'stdvol'] = (df['amount']/1000000).rolling(20).std(ddof=1)

    df1 = df[df['return'] < 0].copy()
    df1.loc[:, 'NEGILLIQ'] = df1['return'].abs()*100 / (df['amount']/100000000)
    df = df.merge(df1, on=list(df.columns), how='left')

    if stk_df is not None:
        ind_temp = stk_df.groupby('date')['cap'].sum().reset_index(level=0)
        df = df.merge(ind_temp, on='date', how='left')
        df['tov'] = df['amount']/df['cap'] * 100
        df['abturnover'] = (df['tov'].rolling(20).mean())/(df['tov'].rolling(120).mean())
        df['stdturnover'] = df['tov'].rolling(20).std(ddof=1)
        return df.loc[:, ['symbol', 'date', 'ILLIQ', 'ABILLIQ', 'NEGILLIQ', 'stdvol', 'abturnover', 'stdturnover']]
    else:
        return df.loc[:, ['symbol', 'date', 'ILLIQ', 'ABILLIQ', 'NEGILLIQ', 'stdvol']]


def volatility_factors(ind_df, window, alpha=0.34):
    """ """
    data = ind_df.sort_values('date').copy()

    data['c2c'] = data['close'].div(data['pre_close'])
    data['ln_c2c'] = np.log(data['c2c'])
    data['c2c_sigma'] = data.groupby('symbol')['ln_c2c'].rolling(window, min_periods=1).std(ddof=1).values

    data['o'] = data['open'].div(data['pre_close'])
    data['ln_o'] = np.log(data['o'])
    data['o_sigma'] = data.groupby('symbol')['ln_o'].rolling(window, min_periods=1).std(ddof=1).values

    data['c'] = data['close'].div(data['open'])
    data['ln_c'] = np.log(data['c'])
    data['c_sigma'] = data.groupby('symbol')['ln_c'].rolling(window, min_periods=1).std(ddof=1).values

    data['h_c'] = np.log(data['high'].div(data['close']))
    data['h_o'] = np.log(data['high'].div(data['open']))
    data['l_c'] = np.log(data['low'].div(data['close']))
    data['l_o'] = np.log(data['low'].div(data['open']))

    data['rsy'] = data['h_c'] * data['h_o'] + data['l_c'] * data['l_o']
    data['rsy_sigma'] = data.groupby('symbol')['rsy'].rolling(window).mean().values
    data['rsy_sigma'] = np.sqrt(data['rsy_sigma'])

    # weighted average
    k = alpha / ((1 + alpha) + (window + 1) / (window - 1))

    data['yang_zhang_sigma'] = np.sqrt(
        pow(data['o_sigma'], 2) + k * pow(data['c_sigma'], 2) + (1 - k) * pow(data['rsy_sigma'], 2))

    return data[['symbol', 'date', 'c2c_sigma', 'rsy_sigma', 'yang_zhang_sigma']]


def Res(ind_df, window):
    """ """
    df = ind_df.copy()
    df['Res_return_1'] = df['low'] / df['open'] - 1
    df['Res_return_2'] = df['close'] / df['low'] - 1
    df['return'] = df['close'] / df['pre_close'] - 1
    df['cov'] = df['Res_return_1'].rolling(window).cov(df['Res_return_2'])
    df['return_var'] = df['return'].rolling(window).var(ddof=1)
    df['Res'] = df['cov'] / df['return_var']

    return df[['symbol', 'date', 'Res']]


def high_moment(ind_df, window):
    """"""
    df = ind_df.copy()
    df['return'] = df['close'] / df['pre_close'] - 1
    df['kurtosis'] = df['return'].rolling(window).apply(lambda x: kurtosis(x))
    df['skew'] = df['return'].rolling(window).apply(lambda x: skew(x))

    return df[['symbol', 'date', 'kurtosis', 'skew']]


def volumes(ind_df):
    """"""
    df = ind_df.copy()
    df['vol5'] = df['volume'].rolling(5).mean()
    df['vol10'] = df['volume'].rolling(10).mean()
    df['vol20'] = df['volume'].rolling(20).mean()
    return df[['symbol', 'date', 'volume', 'vol5', 'vol10', 'vol20']]


def sigma_return_corr(ind_df, sigma_df):
    df = ind_df.merge(sigma_df, on=['symbol','date']).copy()
    df['return'] = df['close'] / df['pre_close'] - 1
    df['sigma_return_corr5'] = df['return'].rolling(5).corr(df['yang_zhang_sigma'])
    df['sigma_return_corr10'] = df['return'].rolling(10).corr(df['yang_zhang_sigma'])
    df['sigma_return_corr20'] = df['return'].rolling(20).corr(df['yang_zhang_sigma'])
    return df[['symbol', 'date', 'sigma_return_corr5', 'sigma_return_corr10', 'sigma_return_corr20']]


def momentum(ind_df):
    df = ind_df.copy()
    df['overnight'] = df['open']/df['pre_close']
    df['intraday'] = df['close']/df['open']
    df['ovn5'] = df['overnight'].rolling(5).sum()
    df['ovn10'] = df['overnight'].rolling(10).sum()
    df['ovn20'] = df['overnight'].rolling(20).sum()
    df['intra5'] = df['intraday'].rolling(5).sum()
    df['intra10'] = df['intraday'].rolling(10).sum()
    df['intra20'] = df['intraday'].rolling(20).sum()
    return df[['symbol', 'date', 'ovn5', 'ovn10', 'ovn20', 'intra5', 'intra10', 'intra20']]