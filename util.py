# coding=utf-8
import datetime as dt
import numpy as np
from math import *
import pandas as pd

def is_season_report(trade_date):
    if trade_date.month == 3 or trade_date.month == 12:
        if trade_date.day == 31:
            return True
    if trade_date.month == 6 or trade_date.month == 9:
        if trade_date.day == 30:
            return True
    return False


def find_tradeday(trade_day, trade_days, offsize=0):
    '''
    得到最近一天的交易日时间
    :param trade_day:
    :param offsize:
    :return:
    '''
    # trade_time = dt.datetime.strptime(trade_day, "%Y-%m-%d") + dt.timedelta(days=offsize)
    trade_time = trade_day + dt.timedelta(days=offsize)
    for date_str in trade_days:
        date_time = dt.datetime.strptime(date_str, "%Y-%m-%d")
        if date_time >= trade_time:
            # return date_time if f_tpype == 'd' else find_seasonday(dt.datetime.strptime(date_str, "%Y-%m-%d"))
            return  date_time

    # return trade_days[-1] if f_tpype == 'd' else find_seasonday(dt.datetime.strptime(trade_days[-1], "%Y-%m-%d"))
    return trade_days[-1]


def find_seasonday(day_date):
    '''
    获得最近一个报告期时间
    :param self:
    :param day:
    :return:
    '''
    while not is_season_report(day_date):
        day_date = day_date - dt.timedelta(days=1)
    return day_date


def get_no_nan(data):
    try:
        if isnan(data):
            return "NULL"
        else:
            return data
    except:
        return "NULL"
    # result = float(0)
    # try:
    #     result = float(data)
    #     return result
    # except:
    #     return "NULL"

# print get_no_nan("0.2g1")


def get_maker(code):
    if code[0] == '6':
        return code+".SH"
    else:
        return code+".SZ"

def get_codemaker_df(df):
    e_lst = []
    for e in df['code'].values:
        e_lst.append(get_maker(e))
    df['code'] = pd.Series(e_lst, index=df.index)
    return df