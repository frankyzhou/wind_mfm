# coding=utf8
from WindPy import *
from init import *
import tushare as ts
import traceback
from util import *
import pandas as pd
# from alpha.stock_holding import get_index_stocks

def get_index_stocks(index, date):
    all_stock = w.wset("indexconstituent","date=%s;windcode=%s" %(date, index)).Data
    stock_lst = []
    for i in range(len(all_stock[1])):
        stock = {}
        stock["code"] = all_stock[1][i]
        stock["percent"] = all_stock[3][i]/100 if all_stock[3][i] != None else 0
        stock_lst.append(stock)
    df = pd.DataFrame(stock_lst)
    df.index = df['code']
    return df

class wind():
    def __init__(self):
        w.start()
        self.dao = dao()
        self.engine = self.dao.get_engine()

    def get_SectorConstituent(self, startdate):
        '''
        获得某个时间点的指数成分股
        :param startdate:
        :return:
        '''
        all_stock = w.wset("SectorConstituent",u"date="+startdate+u";sector=全部A股").Data#取全部A股股票代码、名称信息
        codes = all_stock[1]
        names = all_stock[2]
        num = 0
        for code in codes:
            sql = "select code from %s where code='%s'" % ("stock_info", code)
            result = self.engine.execute(sql)
            if result.rowcount == 0:
                data = w.wss(code, "delist_date, ipo_date, industry2, sec_name",
                             "industryType=1;industryStandard=1;tradeDate=" + startdate).Data
                name = data[3][0]
                industry = data[2][0]
                ipo = data[1][0]
                end = data[0][0]
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

    def get_daily_k(self, startdate, enddate=None, total=0, num=0):
        # trade_days = ts.get_k_data("000001", startdate, enddate)["date"].values
        # for date_str in trade_days:
        if enddate == None:
            enddate = dt.datetime.now().strftime("%Y-%m-%d")
        # all_stock = w.wset("SectorConstituent", u"date=" + enddate + u";sector=全部A股").Data  # 取全部A股股票代码、名称信息
        # codes = all_stock[1]
        i = 0
        print num, total
        sql_stock = "SELECT distinct(code) FROM stock.daily_k"
        lst = pd.read_sql(sql_stock, self.engine)
        count = 0
        len_lst = len(lst['code'])
        avg = len_lst / total
        for code in lst["code"][num*avg:min((num+1)*avg-1, len_lst-1)]:
        # for code in codes[startNo:endNo]:
            # if i < 999:
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
        '''
        通过tushare的数据，将历史的k线数据批量导入数据库
        :param code: 
        :param startdate: 
        :param enddate: 
        :return: 
        '''
        data = ts.get_h_data(code[:-3], start=startdate, end=enddate)
        for i in range(len(data)):
            print '-'*10 + str(i)
            date_str = data.index[i].strftime("%Y-%m-%d")
            try:
                sql_insert = "update daily_k set open=%s, high=%s, close=%s, low=%s where code='%s' and date='%s'" \
                             % (data['open'][i], data['high'][i], data['close'][i], data['low'][i], code, date_str)
                self.engine.execute(sql_insert)
            except:
                self.engine = self.dao.get_engine()
                traceback.print_exc()

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
            last_month = (dt.datetime.strptime(date_str, "%Y-%m-%d") - dt.timedelta(days=30)).strftime("%Y-%m-%d")
            sql_stock = "select code from stock_info where startdate < '%s'" % (date_str)
            # all_stock = w.wset("SectorConstituent", u"date=" + date_str + u";sector=全部A股").Data  # 取全部A股股票代码、名称信息
            # codes = all_stock[1]
            stocks = pd.read_sql(sql_stock, self.engine)
            i = 0
            for code in stocks["code"]:
                # code = '600005.SH'
                try:
                    sql_exist = "select * from daily_factors where code='%s' and date='%s'" % (code, date_str)
                    # result = self.engine.execute(sql_exist)
                    df_stock = pd.read_sql(sql_exist, self.engine)
                except:
                    traceback.print_exc()
                    # result = ""
                    df_stock = pd.DataFrame()
                    self.engine = self.dao.get_engine() # 重新连接

                i += 1
                if i % 100 == 0:
                    print i, date_str

                if len(df_stock)==0 or any(df_stock.values[0][:-1] == [None]):  # 除去动量，至少有一个不存在
                    try:
                        data = w.wss(code, "pe_ttm, pb_lf, dividendyield2, mkt_cap_ard, beta, stdevry",
                              "startDate=%s;endDate=%s;period=1;returnType=1;\
                              index=000001.SH;tradeDate=%s" %(last_month, date_str, date_str)).Data
                        pe  = get_no_nan(data[0][0])
                        pb  = get_no_nan(data[1][0])
                        divide = get_no_nan(data[2][0])
                        mktcap = get_no_nan(data[3][0])
                        beta = get_no_nan(data[4][0])
                        std = get_no_nan(data[5][0])
                        if len(df_stock)==0: # 全部不存在
                            sql_insert = "insert into daily_factors (code, date, pe, pb , divide, mktcap, beta, std) \
                            values ('%s', '%s', %s, %s, %s, %s, %s, %s)" % (code, date_str, pe, pb, divide, mktcap, beta, std)
                        else:
                            sql_insert = "update daily_factors set pe=%s, pb=%s, divide =%s, mktcap=%s, beta=%s, std=%s where code='%s' and date='%s'" \
                                         % (pe, pb, divide, mktcap, beta, std, code, date_str)
                        self.engine.execute(sql_insert)
                    except:
                        traceback.print_exc()
                        self.engine = self.dao.get_engine()  # 重新连接
                # break

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

                    # if result == "" or result.rowcount == 0:  # 不存在
                    try:
                        data = w.wss(code, "qfa_tot_oper_rev, qfa_grossmargin, qfa_net_profit_is, qfa_opprofit,\
                                        eps_ttm, current, debttoassets, cashtocurrentdebt, roe_avg, roa, ebitda2",
                                     "unit=1;rptType=1;PriceAdj=F;rptDate=" + date_str).Data

                        total_rev = get_no_nan(data[0][0])
                        gross_margin = get_no_nan(data[1][0])
                        profit = get_no_nan(data[2][0])
                        oppo_profit = get_no_nan(data[3][0])
                        eps = get_no_nan(data[4][0])
                        current = get_no_nan(data[5][0])  # 流动比率
                        debt_asset = get_no_nan(data[6][0])  # 资产负债率
                        cash_debt = get_no_nan(data[7][0])  # 现金比率 流动现金/流动负债
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

    def get_momentum(self, startdate, enddate, total, num):
        '''
        获得股票的一月动量，并写入数据库中
        :return:
        '''
        print num, total
        sql_stock = "SELECT distinct(code) FROM stock.daily_k"
        lst = pd.read_sql(sql_stock, self.engine)
        count = 0
        len_lst = len(lst['code'])
        avg = len_lst / total
        for code in lst["code"][num*avg:min((num+1)*avg-1, len_lst-1)]:
            count += 1
            if count % 10 == 0:
                print count
            # if count < 300:
            #     continue
            sql = "select date, close from daily_k where code = '%s' and date > '%s' and date < '%s' order by date" % (code, startdate, enddate)
            df = pd.read_sql(sql, self.engine)
            series = df['close']/df['close'].shift(20) - 1
            for i in range(len(df)):
                if not np.isnan(series[i]):
                    try:
                        sql = "insert into daily_factors (code, date, moment_1m) \
                                                                          values('%s', '%s', % s)" % (
                        get_maker(code), df["date"][i], get_no_nan(series[i]))
                        self.engine.execute(sql)
                    except:
                        sql = "update daily_factors set moment_1m=%s where code='%s' and date='%s'"\
                              % (get_no_nan(series[i]), get_maker(code), df["date"][i])
                        self.engine.execute(sql)
                        # self.engine = self.dao.get_engine()  # 重新连接

    def update_index_percent(self, index, startdate, enddate):
        trade_days = ts.get_k_data("000001", startdate, enddate)["date"].values
        df_old = []
        for tmp in trade_days:
            # print tmp
            df = get_index_stocks(index, tmp)
            df['index_code'] = index
            df['date'] = tmp
            df.index = range(len(df))
            if len(df_old) == 0:
                df.to_sql("index_stock", self.engine, if_exists='append')
                df_old = df.copy()
                print tmp
            else:
                if not (df_old[['code', 'percent']].values == df[['code', 'percent']].values).all():
                    df_old = df.copy()
                    df.to_sql("index_stock", self.engine, if_exists='append')
                    print tmp


while 1:
    try:
        wind_ins = wind()
        # wind_ins.get_SectorConstituent("2017-07-10")
        # wind_ins.get_daily_k("2016-01-01", total=2, num=0)
        # wind_ins.get_daily_factor("2017-07-11", "2017-07-31")
        wind_ins.get_quarter_factor("2017-03-01", "2017-09-01")
        # wind_ins.cal_quarter_growth("2015-06-01", "2016-02-01")

        # wind_ins.get_momentum("2015-09-01", "2016-01-01", 5, 0)
        # wind_ins.update_index_percent('000300.SH', '2015-01-01', '2017-07-07')
        break
    except:
        traceback.print_exc()
# wind_ins.get_daily_k("2017-03-01", "2017-04-01")
# wind_ins = wind()
# wind_ins.insert_k_data("002428", "2017-03-01", "2017-04-01")