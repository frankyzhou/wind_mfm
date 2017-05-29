# coding=utf-8
import xlrd
from init import *
import pandas as pd
import matplotlib.pyplot as plt
from WindPy import *
from getFactor import *
import os

w.start()
factors_not_industry = ['mktcap', 'divide', 'current', 'debt_asset', 'close', 'turn']
factors_d = ['pe', 'pb', 'divide', 'mktcap', 'beta', 'std']
factors_barra = ['value', 'debt', 'moment_1m', 'turnover', 'mktcap', 'std', 'beta', 'growth']

def open_excel(file= 'file.xls'):
    try:
        data = xlrd.open_workbook(file)
        return data
    except Exception,e:
        print str(e)


#根据索引获取Excel表格中的数据   参数:file：Excel文件路径     colnameindex：表头列名所在行的所以  ，by_index：表的索引
def excel_table_byindex(file,colnameindex,by_index=0):
    data = open_excel(file)
    table = data.sheets()[by_index]
    nrows = table.nrows #行数
    ncols = table.ncols #列数
    colnames =  table.row_values(colnameindex) #某一行数据
    list =[]
    for rownum in range(1,nrows):
         row = table.row_values(rownum)
         if row:
             app = {}
             for i in range(len(colnames)):
                app[colnames[i]] = row[i]
             list.append(app)
    return list


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


def get_stock_holding(name):
    stock_lst = []
    code = u'证券代码'
    percent = u'持仓市值(净价)/单元净值(%)'
    tables = excel_table_byindex(name+'.xls', 0)
    sum_percent = 0
    for row in tables:
        # if row[code][:4] == '1102' and len(row[code]) > 10:
        #     stock = {}
        #     stock_code = row[code][-6:]
        #     maker = "H" if stock_code[0] == '6' else "Z"
        #     stock["code"] = stock_code + ".S" + maker
        #     stock["percent"] = row[percent]
        #     sum_percent += row[percent]
        #     stock_lst.append(stock)
        stock = {}
        stock_code = row[code]
        if len(stock_code) == 6 and stock_code[0] in ['0','3','6']:
            maker = "H" if stock_code[0] == '6' else "Z"
            stock["code"] = stock_code + ".S" + maker
            stock["percent"] = row[percent]
            sum_percent += row[percent]
            stock_lst.append(stock)
    if sum_percent > 0:
        for stock in stock_lst:
            stock["percent"] = stock["percent"] / sum_percent
    df = pd.DataFrame(stock_lst)
    if len(df) > 0:
        df.index = df['code']
    return df


def get_cap_percent(df, stock_df):
    cap_pct = [0] * 3
    try:
        for code in stock_df['code'].values:
            cap = df[df.code == code]["mktcap"].values[0] / 100000000
            if cap < 100:
                cap_pct[0] += stock_df["percent"][code]
            elif cap < 500 and cap > 100:
                cap_pct[1] += stock_df["percent"][code]
            else:
                cap_pct[2] += stock_df["percent"][code]
        sum_percent = sum(cap_pct)
        if sum_percent > 0:
            for i in range(len(cap_pct)):
                cap_pct[i] = cap_pct[i] / sum_percent
    except:
        print 1
    return cap_pct


def get_indu_percent(df, stock_df):
    industry_set = set()
    sum_lst = 0
    for i in df["industry"].values:
        if i != "None":
            industry_set.add(i)
    industry_lst = list(industry_set)
    # industry_lst = df["industry"].drop_duplicates().values
    industry_pct = [0] * len(industry_lst)
    if len(stock_df) > 0:
        for code in stock_df['code'].values:
            industry = df[df.code == code]["industry"].values[0]
            i = industry_lst.index(industry)
            industry_pct[i] += stock_df["percent"][code]

    sum_lst = sum(industry_pct)
    if sum_lst > 0:
        for i in range(len(industry_pct)):
            industry_pct[i] = industry_pct[i] / sum_lst
    return industry_pct, industry_lst


def find_index_rank(index, code):
    for i in range(len(index)):
        if index[i] == code:
            return float(i)/len(index)
    return 0.5


class stock_holding():
    def __init__(self):
        self.db = dao()
        self.engine = self.db.get_engine()
        self.cal = cal_return()

    def get_cap_percent(self, stock_lst, bench_stocklst, date_str):
        '''
        获取投顾在某个时期的大小盘敞口
        :param stock_lst:
        :param bench_stocklst:
        :param date_str:
        :return:
        '''
        df_dict = {}

        sql = "select code, mktcap from daily_factors where date='%s'" % date_str
        df = pd.read_sql(sql, self.engine)

        df_dict["HS300"] = get_cap_percent(df, bench_stocklst)
        df_dict["Portfolio"] = get_cap_percent(df, stock_lst)

        df_cap = pd.DataFrame(df_dict)
        df_cap.index = ["小盘股", "中盘股", "大盘股"]
        # plt.figure()
        # df_cap.plot.bar()
        # plt.savefig("cap_pct.png")
        return df_cap

    def get_inds_percent(self, stock_lst, bench_stocklst):
        '''
        获取投顾在不同行业的敞口
        :param stock_lst:
        :param bench_stocklst:
        :return:
        '''
        df_dict = {}

        sql = "select code, industry from stock_info"
        df = pd.read_sql(sql, self.engine)

        df_dict["HS300"], indu_lst = get_indu_percent(df, bench_stocklst)
        df_dict["Portfolio"], indu_lst = get_indu_percent(df, stock_lst)
        df = pd.DataFrame(df_dict, index=indu_lst)
        # df.plot.bar(figsize=[15,8])
        # plt.savefig("indu_pct.png")
        return df

    def get_factor_percent(self, stock_lst, date_str):
        stock_df = self.cal.get_stock(dt.datetime.strptime(date_str, "%Y-%m-%d"), "d")
        stock_df = self.merge_industry(stock_df)
        stock_df.index = stock_df['code']

        stock_df_s = self.cal.get_stock(dt.datetime.strptime(date_str, "%Y-%m-%d"), "s")
        stock_df_s = self.merge_industry(stock_df_s)
        stock_df_s.index = stock_df_s['code']

        stock_df_k = self.cal.get_stock(dt.datetime.strptime(date_str, "%Y-%m-%d"), "k")
        stock_df_k = get_codemaker_df(stock_df_k)
        stock_df_k = self.merge_industry(stock_df_k)
        stock_df_k.index = stock_df_k['code']

        df = pd.merge(stock_df.iloc[:, 1:], stock_df_s.iloc[:, 1:], left_index=True, right_index=True)
        df = pd.merge(df, stock_df_k.iloc[:, 2:], left_index=True, right_index=True)
        df = df.fillna(0)
        del df['date_y']
        del df['date_x']
        del df['industry_y']
        df['debt'] = (df['current'] + df['debt_asset']) / 2
        df['value'] = (df['pe'] + df['pb'] + df['divide']) / 3
        df['growth'] = (df['total_rev_g'] + df['gross_margin_g'] + df['oppo_profit_g'] + df['profit_g']) / 4
        # df_new = df[['industry_x', 'value', 'debt', 'moment_1m', 'turnover', 'mktcap', 'std', 'beta', 'growth']]

        factor_load = {}
        for f in factors_barra:
            factor_load[f] = sum((df[f] * stock_lst['percent']).fillna(0)) if len(stock_lst) > 0 else 0
        return pd.DataFrame([factor_load])

    def merge_industry(self, stock_df):
        sql_getindustry = "select code, industry from stock_info"
        df_code = pd.read_sql(sql_getindustry, self.engine)
        df = pd.merge(df_code, stock_df)  # 通过merge剔除新股
        return self.cal.industry_factor(df)

    def get_factor_return(self, date_str):
        sql_k = "select code, close from daily_k where date = '%s' order by code" % (date_str)
        df_k = pd.read_sql(sql_k, self.engine)

# plt.figure()
# plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
# plt.rcParams['axes.unicode_minus']=False #用来正常显示负号

file_lst = os.listdir('lianghua3')
for name in file_lst:
    if len(name) < 5:
        continue
    date = name.split('.')[0]
    print date
    stock_df = get_stock_holding('lianghua3/' + date)
    bench_stock_df = get_index_stocks('000300.SH', date)
    s_h = stock_holding()
    cap_df = s_h.get_cap_percent(stock_df, bench_stock_df, date)
    indu_df = s_h.get_inds_percent(stock_df, bench_stock_df)

    fact_df_porfolio = s_h.get_factor_percent(stock_df, date)
    fact_df_benchmark = s_h.get_factor_percent(bench_stock_df, date)
    fact_df = pd.concat([fact_df_porfolio, fact_df_benchmark])
    fact_df.index = ['p','b']

    cap_df.to_csv('lianghua3/data/cap/cap_df_'+date+'.csv',encoding='utf-8')
    indu_df.to_csv('lianghua3/data/indu/indu_df_'+date+'.csv',encoding='utf-8')
    fact_df.to_csv('lianghua3/data/fact/fact_df_'+date+'.csv',encoding='utf-8')

print 1
