# coding=utf-8
from init import *
from pandas import Series, DataFrame
import numpy as np
from WindPy import *
import tushare as ts
import traceback
from util import *
import pandas as pd

class cal_return():
    def __init__(self):
        w.start()
        self.dao = dao()
        self.engine = self.dao.get_engine()

    def get_return_by_port(self, l1, startDate, nextDate):
        '''
        获取当前组合的两周期投资差
        :param l1:
        :param startDate:
        :param nextDate:
        :param endDate:
        :return:
        '''
        price_new = self.get_price(l1, nextDate.strftime("%Y-%m-%d"))
        price_old = self.get_price(l1, startDate.strftime("%Y-%m-%d"))
        df = pd.merge(price_new, price_old, on="code", how="outer")  #合并行情
        ratio = df.ix[:,1] / df.ix[:,2]
        return ratio.sum() / len(ratio)

    def get_split(self, df, indicator):
        '''
        将投资组合分组
        :param df:
        :param indicator:
        :return:
        '''
        sort = df[indicator].dropna().order()
        #     print sort
        l = len(sort)
        sort = list(sort.index)
        l1 = sort[0:l / 5 - 1]
        l2 = sort[l / 5:2 * l / 5 - 1]
        l3 = sort[2 * l / 5:3 * l / 5 - 1]
        l4 = sort[3 * l / 5:4 * l / 5 - 1]
        l5 = sort[4 * l / 5:l - 1]
        return l1, l2, l3, l4, l5

    def get_return_ports(self, st, size, indicator, f_type, time_length):
        '''
        得到五组的投资回报
        :param st:
        :param size:
        :param indicator:
        :return:
        '''
        result = {}
        result["1"] = 1
        result["2"] = 1
        result["3"] = 1
        result["4"] = 1
        result["5"] = 1
        st = find_tradeday(dt.datetime.strptime(st, "%Y-%m-%d"), trade_days)

        for i in range(size):
            startDate = st + dt.timedelta(days=time_length * i)  # 开始时间,size作为基差
            startDate = find_tradeday(startDate, trade_days)  # 调整为最近一个交易日
            df = self.get_stock(startDate, f_type)  # 根据频率,调整选股，若频率为日，则选择最近一个交易日，若频率是季度，则选择之前一个最近的季度

            sql_getindustry = "select code, industry from stock_info where startdate < '%s'" % (startDate - dt.timedelta(days=365)).strftime("%Y-%m-%d")
            df_code = pd.read_sql(sql_getindustry, self.engine)
            df = pd.merge(df_code, df)  #通过merge剔除新股
            df = self.industry_factor(df)
            df.index = df["code"]
            df = df.iloc[:, 1:]

            l1, l2, l3, l4, l5 = self.get_split(df, indicator)  # 按比重切分股票池
            nextDate = find_tradeday(startDate, trade_days, time_length)    # 距离本期开始的time_length下的最近一个交易日
            result["1"] = self.get_return_by_port(l1, startDate, nextDate) * (result["1"])
            result["2"] = self.get_return_by_port(l2, startDate, nextDate) * (result["2"])
            result["3"] = self.get_return_by_port(l3, startDate, nextDate) * (result["3"])
            result["4"] = self.get_return_by_port(l4, startDate, nextDate) * (result["4"])
            result["5"] = self.get_return_by_port(l5, startDate, nextDate) * (result["5"])
        return Series(result)

    def get_price(self, lst, date):
        df_list = []
        lst_str = ""
        for i in lst:
            lst_str = lst_str + "'" + i + "',"
        sql = "select code, close from daily_k where code in (%s) and date ='%s' order by code" % (lst_str[:-1], date)
        df = pd.read_sql(sql, self.engine)
        return df

    def get_stock(self, fdate, f_type):
        '''
        得到股票的多因子序列
        有三种：
        d:每日因子
        s:每季度因子
        :param fdate:
        :param factors:
        :return:
        '''
        # 获取基础数据
        table = "daily"
        factors = factors_d
        if f_type == 's':
            fdate = find_seasonday(fdate)
            table = "season"
            factors = factors_s

        fdate = fdate.strftime("%Y-%m-%d")
        sql = "select * from " + table + "_factors where date = '%s'" % fdate

        df = pd.read_sql(sql, self.engine)

        if f_type == 'd':
            df["pe"] = 1/(df["pe"])  # 将Pe转换为倒数
        return df

    def industry_factor(self, df):
        industry_set = set()
        df_list = []
        for i in df["industry"].values:
            industry_set.add(i)
        for i in industry_set:
            df_i = df[df.industry == i]
            for c in df_i.columns[3:-4]:
                if c not in factors_not_industry:
                    df_i[c] = (df_i[c] - df_i[c].mean()) / df_i[c].std()
            df_list.append(df_i)
        df_all = pd.concat(df_list)
        return df_all


# lst = ['000001', '000002']
cal = cal_return()
# for code in lst:
#     cal.get_price(lst, "2017-03-01", "2017-03-10")

# factors_d = ['pe', 'pb', 'divide', 'mktcap']
factors_d = ['mktcap']
factors_s = ['total_rev', 'gross_margin', 'profit', 'eps', 'current', 'debt_asset',\
             'cash_debt', 'oppo_profit', 'roe', 'roa', 'eb', 'total_rev_g', 'gross_margin_g', \
             'profit_g', 'oppo_profit_g']
factors_not_industry = ['mktcap', 'divide', 'current', 'debt_asset']
startdate = "2017-03-01"
enddate = "2017-05-01"
trade_days = ts.get_k_data("000001", startdate, enddate)["date"].values

for fact in factors_d:
    print fact
    print cal.get_return_ports(startdate, 5, fact, 'd', 10)

# for fact in factors_s:
#     print fact
#     print cal.get_return_ports(startdate, 1, fact, "s", 30)

# cal.get_stock("2017-03-30", "s")
#
# a = np.array([1, np.nan])
# df = DataFrame(a)
# t = df.dropna()
# print 1
