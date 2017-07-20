# coding=utf-8
from init import *
from pandas import Series
from WindPy import *
import tushare as ts
import traceback
from util import *
import pandas as pd
import matplotlib.pyplot as plt

factors_not_industry = ['mktcap', 'divide', 'current', 'debt_asset', 'turn']

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
        try:
            ratio = df.ix[:,1] / df.ix[:,2]
        except:
            return np.nan
        return ratio.sum() / len(ratio)

    def get_split(self, df, indicator):
        '''
        将投资组合分组
        :param df:
        :param indicator:
        :return:
        '''
        sort = df[indicator].dropna().order()
        l = len(sort)
        sort = list(sort.index)
        l1 = sort[0:l / 5 - 1]
        l2 = sort[l / 5:2 * l / 5 - 1]
        l3 = sort[2 * l / 5:3 * l / 5 - 1]
        l4 = sort[3 * l / 5:4 * l / 5 - 1]
        l5 = sort[4 * l / 5:l]
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

            sql_getindustry = "select code, industry from stock_info where startdate < '%s'" % (startDate - dt.timedelta(days=250)).strftime("%Y-%m-%d")
            df_code = pd.read_sql(sql_getindustry, self.engine)
            df = pd.merge(df_code, df)  #通过merge剔除新股
            if len(df) == 0:
                pass
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

    def get_return_ports_into_db(self, st, size, indicator, f_type):
        '''
        得到五组的投资回报
        :param st:
        :param size:
        :param indicator:
        :return:
        '''

        for i in range(min(size, len(trade_days))):
            startDate = dt.datetime.strptime(trade_days[i], "%Y-%m-%d")
            nextDate_1d = dt.datetime.strptime(trade_days[i+1], "%Y-%m-%d") if i < len(trade_days)-1 else startDate + dt.timedelta(days=1)
            nextDate_1w = dt.datetime.strptime(trade_days[i+5], "%Y-%m-%d") if i < len(trade_days)-5 else startDate + dt.timedelta(days=7)
            nextDate_1m = dt.datetime.strptime(trade_days[i+20], "%Y-%m-%d") if i < len(trade_days)-20 else startDate + dt.timedelta(days=30)
            df = self.get_stock(startDate, f_type)  # 根据频率,调整选股，若频率为日，则选择最近一个交易日，若频率是季度，则选择之前一个最近的季度

            sql_getindustry = "select code, industry from stock_info where startdate < '%s'" % (startDate - dt.timedelta(days=250)).strftime("%Y-%m-%d")
            df_code = pd.read_sql(sql_getindustry, self.engine)
            df = pd.merge(df_code, df)  # 通过merge剔除新股
            df = self.industry_factor(df)
            df.index = df["code"]
            df = df.iloc[:, 1:]  # 后几列

            l1, l2, l3, l4, l5 = self.get_split(df, indicator)  # 按比重切分股票池
            return_1d = self.get_return_by_port(l1, startDate, nextDate_1d) - self.get_return_by_port(l5, startDate, nextDate_1d)
            return_1w = self.get_return_by_port(l1, startDate, nextDate_1w) - self.get_return_by_port(l5, startDate, nextDate_1w)
            return_1m = self.get_return_by_port(l1, startDate, nextDate_1m) - self.get_return_by_port(l5, startDate, nextDate_1m)

            return_1d = - return_1d if indicator in factors_positive else return_1d
            return_1w = - return_1w if indicator in factors_positive else return_1w
            return_1m = - return_1m if indicator in factors_positive else return_1m

            sql_return = "insert into factor_return (factor, date, return_1d, return_1w, return_1m) \
            values ('%s', '%s', %s, %s, %s)" % (indicator, startDate.strftime("%Y-%m-%d"), get_no_nan(return_1d),\
                                                      get_no_nan(return_1w), get_no_nan(return_1m))
            try:
                self.engine.execute(sql_return)
            except:
                traceback.print_exc()
                print indicator + " in %s has exisit" % startDate.strftime("%Y-%m-%d")
                sql_return = "update factor_return set  return_1d=%s, return_1w=%s, return_1m=%s where factor = '%s' and date='%s'"\
                % (get_no_nan(return_1d),get_no_nan(return_1w), get_no_nan(return_1m), indicator, startDate.strftime("%Y-%m-%d"))
                self.engine.execute(sql_return)
            print startDate.strftime("%Y-%m-%d")

    def get_price(self, lst, date):
        '''
        得到列表lst在date时间下的行情序列
        :param lst:
        :param date:
        :return:
        '''
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
        # fdate = dt.datetime.strptime(fdate, "%Y-%m-%d")
        table = "daily"
        if f_type == 's':
            fdate = find_seasonday(fdate)
            table = "season"

        # fdate = fdate.strftime("%Y-%m-%d")

        sql = "select * from " + table + "_factors where date = '%s'" % fdate if f_type != 'k' else \
            "select code, close, turnover from daily_k where date = '%s'" % fdate

        df = pd.read_sql(sql, self.engine)

        if f_type == 'd':
            df["pe"] = 1/(df["pe"])  # 将Pe转换为倒数
            df["pb"] = 1/(df["pb"])
            df["mktcap"] = np.log(df.mktcap)
            # df["beta"] = 1/abs(df["beta"])
            # df["std"] = 1 / (df["std"])
        if f_type == 's':
            df['current'] = 1 / df['current']
        return df

    def industry_factor(self, df):
        '''
        行业中性化，分两种；
        一种是分行业，一种是全市场
        :param df:
        :return:
        '''
        industry_set = set()
        df_list = []
        for i in df["industry"].values:
            industry_set.add(i)
        for i in industry_set:
            df_i = df[df.industry == i]
            for c in df_i.columns[3:]:
                if c not in factors_not_industry:
                    if df_i[c].std() != 0 and type(df_i[c].std()) == float:
                        df_i[c] = (df_i[c] - df_i[c].mean()) / df_i[c].std()
                    else:
                        df_i[c] = 0
            df_list.append(df_i)
        if len(df_list) == 0:
            pass
        df_all = pd.concat(df_list)
        for c in factors_not_industry:
            try:
                df_all[c] = (df_all[c] - df_all[c].mean()) / df_all[c].std()
            except:
                continue
        return df_all

    def get_return_series(self, factors):
        '''
        绘制因子收益率
        :param factors:
        :return:
        '''
        lst = []
        for f in factors:
            sql_2 = "select * from factor_return where factor ='%s'" % f
            df_2 = pd.read_sql(sql_2, self.engine)
            df_2 = df_2.loc[:,['date','return_1d']]
            df_2.index = df_2["date"]
            for i in range(1, len(df_2["return_1d"])):
                df_2["return_1d"][i] = (df_2["return_1d"][i-1] + 1) * (df_2["return_1d"][i] + 1) - 1
            df_2 = df_2.rename(columns={'return_1d':f + "_" + 'return_1d'})
            lst.append(df_2)
        df = pd.concat(lst, axis=1)
        df.plot(figsize=[15,8])
        plt.savefig("return_series.png")


if __name__ == "__main__":
    cal = cal_return()

    # factors_d = ['pe', 'pb', 'divide', 'mktcap','beta', 'std']
    factors_d = ['moment_1m']
    factors_k = ['turn']
    # factors_s = ['total_rev', 'gross_margin', 'profit', 'eps', 'current', 'debt_asset',\
    #              'cash_debt', 'oppo_profit', 'roe', 'roa', 'eb', 'total_rev_g', 'gross_margin_g',\
    #              'profit_g', 'oppo_profit_g']
    factors_s = ['total_rev_g', 'gross_margin_g', 'profit_g', 'oppo_profit_g']


    factors_positive = ['divide', 'pe']

    startdate = "2017-02-01"
    enddate = "2017-05-01"
    trade_days = ts.get_k_data("000001", startdate, enddate)["date"].values

    # for fact in factors_d:
    #     print fact
    #     print cal.get_return_ports(startdate, 50, fact, 'd', 1)

    # for fact in factors_k:
    #     print fact
    #     print cal.get_return_ports(startdate, 160, fact, 'k', 1)

    # for fact in factors_s:
    #     print fact
    #     print cal.get_return_ports(startdate, 1, fact, "s", 30)

    # for fact in factors_d:
    #     print fact
    #     print cal.get_return_ports_into_db(startdate, 160, fact, "d")

    # for fact in factors_s:
    #     print fact
    #     print cal.get_return_ports_into_db(startdate, 160, fact, "s")

    for fact in factors_k:
        print fact
        print cal.get_return_ports_into_db(startdate, 160, fact, "k")

    # cal.get_return_series(factors_d)

