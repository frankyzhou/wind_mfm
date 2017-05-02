# coding=utf8
from WindPy import *
from init import *
import tushare as ts
import traceback
from util import *
import pandas as pd


class wind():

    def __init__(self):
        w.start()
        self.dao = dao()
        self.engine = self.dao.get_engine()

    def get_SectorConstituent(self, startdate):
        all_stock = w.wset("SectorConstituent",u"date="+startdate+u";sector=全部A股").Data#取全部A股股票代码、名称信息
        codes = all_stock[1]
        names = all_stock[2]
        num = 0
        for code in codes:
            data = w.wss(code, "delist_date, ipo_date, industry_sw, sec_name", "industryType=1;industryStandard=1;tradeDate="+startdate).Data
            name = data[3][0]
            industry = data[2][0]
            ipo = data[1][0]
            end = data[0][0]
            sql = "select code from %s where code='%s'" % ("stock_info", code)
            result = self.engine.execute(sql)
            if result.rowcount == 0:
                if ipo < end:  # 退市
                    sql_insert = "insert into stock_info (code, name,startdate, enddate, industry) \
                            values ('%s', '%s', '%s', '%s', '%s')" % (code, name, ipo, end, industry)
                else:
                    sql_insert = "insert into stock_info (code, name, startdate, industry) \
                                            values ('%s', '%s', '%s', '%s')" % (code, name, ipo, industry)
                self.engine.execute(sql_insert)

            num += 1
            if num % 100 == 0:
                print num

    def get_daily_k(self, startdate, enddate):
        # trade_days = ts.get_k_data("000001", startdate, enddate)["date"].values
        # for date_str in trade_days:
        all_stock = w.wset("SectorConstituent", u"date=" + enddate + u";sector=全部A股").Data  # 取全部A股股票代码、名称信息
        codes = all_stock[1]
        i = 0
        for code in codes:
            # if i < 2399:
            #     i += 1
            #     continue
            try:
                self.insert_k_data(code, startdate, enddate)
            except:
                traceback.print_exc()
            i += 1
            if i % 100 == 0:
                print i

    def insert_k_data(self, code, startdate, enddate):
        data = w.wsd(code, "open,high,low,close,volume,turn", startdate, enddate, "PriceAdj=F")
        for i in range(len(data.Times)):
            date_str = data.Times[i].strftime("%Y-%m-%d")
            try:
                sql_exist = "select code from daily_k where code='%s' and date='%s'" % (code, date_str)
                result = self.engine.execute(sql_exist)
            except:
                result = ""
                traceback.print_exc()
                self.engine = self.dao.get_engine()

            if result == "" or result.rowcount == 0:
                try:
                    sql_insert = "insert into daily_k (code, date, open, close, high, low, volume, turn) values ('%s', '%s', %s, %s, %s, %s, %s, %s) " \
                                 % (code, date_str, get_no_nan(data.Data[0][i]), get_no_nan(data.Data[1][i]),get_no_nan(data.Data[2][i]),\
                                    get_no_nan(data.Data[3][i]), get_no_nan(data.Data[4][i]), get_no_nan(data.Data[5][i]))
                    self.engine.execute(sql_insert)
                except:
                    self.engine = self.dao.get_engine()
                    traceback.print_exc()
            # else:
            #     break  # 已经存在，跳出

    def get_daily_factor(self, startdate, enddate):
        """
        获取每日更新的因子
        ROE,
        ROA,
        PE,
        PB,
        EBITDA,
        TURN,
        DIVIDE,
        MKTCAP
        :param startdate:
        :return:
        """
        trade_days = ts.get_k_data("000001", startdate, enddate)["date"].values
        for date_str in trade_days:
            all_stock = w.wset("SectorConstituent", u"date=" + date_str + u";sector=全部A股").Data  # 取全部A股股票代码、名称信息
            codes = all_stock[1]
            i = 0
            for code in codes:
                try:
                    sql_exist = "select code from daily_factor where code='%s' and date='%s'" % (code, date_str)
                    result = self.engine.execute(sql_exist)
                except:
                    traceback.print_exc()
                    result = ""
                    self.engine = self.dao.get_engine() # 重新连接

                i += 1
                if i % 100 == 0:
                    print i, date_str

                if result == "" or result.rowcount == 0 :  # 不存在
                    try:
                        data = w.wss(code, "pe_ttm, pb_lf, dividendyield2, mkt_cap_ard",
                              "rptDate=20161231;tradeDate=" + date_str).Data
                        pe  = get_no_nan(data[0][0])
                        pb  = get_no_nan(data[1][0])
                        divide = get_no_nan(data[2][0])
                        mktcap = get_no_nan(data[3][0])
                        sql_insert = "insert into daily_factor (code, date, pe, pb , divide, mktcap) \
                                                values ('%s', '%s', %s, %s, %s, %s)" \
                                     % (code, date_str, pe, pb, divide, mktcap)
                        self.engine.execute(sql_insert)
                    except:
                        traceback.print_exc()
                        self.engine = self.dao.get_engine()  # 重新连接

    def get_quarter_factor(self, startdate, enddate):
        """
        获取每季更新的因子
        营业总收入
        净利润
        毛利润
        营业利润
        EPS
        流动比率
        总资产流转率
        负债资产率
        现金比率
        :param startdate:
        :return:
        """
        trade_days = pd.date_range(startdate, enddate)
        for date_str in trade_days:
            if is_season_report(date_str):
                date_str = date_str._date_repr
                all_stock = w.wset("SectorConstituent", u"date=" + date_str + u";sector=全部A股").Data  # 取全部A股股票代码、名称信息
                codes = all_stock[1]
                i = 0
                for code in codes:
                    try:
                        sql_exist = "select code from season_factors where code='%s' and date='%s'" % (code, date_str)
                        result = self.engine.execute(sql_exist)
                    except:
                        traceback.print_exc()
                        result = ""
                        self.engine = self.dao.get_engine()  # 重新连接

                    i += 1
                    if i % 100 == 0:
                        print i, date_str

                    if result == "" or result.rowcount == 0:  # 不存在
                        try:
                            data = w.wss(code, "qfa_tot_oper_rev, qfa_grossmargin, qfa_net_profit_is, qfa_opprofit,\
                                            eps_ttm, current, debttoassets, cashtocurrentdebt, roe_avg, roa, ebitda2",
                                         "unit=1;rptType=1;PriceAdj=F;rptDate=" + date_str).Data

                            total_rev = get_no_nan(data[0][0])
                            gross_margin = get_no_nan(data[1][0])
                            profit = get_no_nan(data[2][0])
                            oppo_profit = get_no_nan(data[3][0])
                            eps = get_no_nan(data[4][0])
                            current = get_no_nan(data[5][0])
                            debt_asset = get_no_nan(data[6][0])
                            cash_debt = get_no_nan(data[7][0])
                            roe = get_no_nan(data[8][0])
                            roa = get_no_nan(data[9][0])
                            eb = get_no_nan(data[10][0])
                            sql_insert = "insert into season_factors (code, date, total_rev, gross_margin, profit, eps, current, debt_asset, cash_debt, oppo_profit, roe, roa, eb) \
                                                            values ('%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
                                         % (code, date_str, total_rev, gross_margin, profit, eps, current, debt_asset, cash_debt, oppo_profit, roe, roa, eb)
                            self.engine.execute(sql_insert)
                        except:
                            traceback.print_exc()
                            self.engine = self.dao.get_engine()  # 重新连接

    def cal_quarter_growth(self, startdate, enddate):
        """
        获取季度增长率
        :param startdate:
        :param enddate:
        :return:
        """
        sql_getcode = "select code from stock_info"
        df_code = pd.read_sql(sql_getcode, self.engine)
        num = 0

        for code in df_code.values:
            code = code[0]
            num += 1
            sql_select = "select total_rev, gross_margin, profit, oppo_profit, date from season_factors where code='%s' order by date DESC" % code
            df = pd.read_sql(sql_select, self.engine)

            info_dict = {1: [], 2: [], 3: [], 0: []}

            for i in range(4):
                tmp_arr = df.values[:,i]
                for j in range(len(tmp_arr)-1):
                    try:
                        info_dict[i].append((float(tmp_arr[j]) - float(tmp_arr[j+1]))/(abs(float(tmp_arr[j+1])))) #防止第二个为负值，差值除以绝对值
                    except:
                        info_dict[i].append("nan")  # 出错就需要添加nan

            for k in range(len(info_dict[0])):
                sql_update = "update season_factors set total_rev_g=%s, gross_margin_g=%s, profit_g=%s, oppo_profit_g=%s where code='%s' and date='%s'" \
                             %(get_no_nan(info_dict[0][k]), get_no_nan(info_dict[1][k]), get_no_nan(info_dict[2][k]), \
                               get_no_nan(info_dict[3][k]), code, df.values[:,4][k])
                self.engine.execute(sql_update)

            if num % 100 ==0:
                print num

while 1:
    try:
        wind_ins = wind()
        # wind_ins.get_SectorConstituent("2017-04-19")
        wind_ins.get_daily_k("2017-04-20", "2017-05-02")
        # wind_ins.get_daily_factor("2017-04-20", "2017-04-21")
        # wind_ins.get_quarter_factor("2016-01-01", "2017-05-01")
        # wind_ins.cal_quarter_growth("2016-01-01", "2017-05-01")
        break
    except:
        traceback.print_exc()
# wind_ins.get_daily_k("2017-03-01", "2017-04-01")
# wind_ins = wind()
# wind_ins.insert_k_data("002428", "2017-03-01", "2017-04-01")