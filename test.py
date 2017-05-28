# coding=utf-8
import tushare as ts
import pandas as pd
from init import *
import numpy as np
from WindPy import *

# df1 = ts.get_report_data(2015, 1)
# df2 = ts.get_report_data(2015, 2)
# print 1
e = dao()
# sql = "select code, date, close from daily_k where code='000001.SZ' and date = '2017-04-06'"
# df = pd.read_sql(sql, e.get_engine())
#
# sql_1 = "select code, date, close from daily_k where code='000002.SZ' and date = '2017-04-06'"
# df_1 = pd.read_sql(sql_1, e.get_engine())

# sql_2 = "select * from factor_return where factor ='pe'"
# df_2 = pd.read_sql(sql_2, e.get_engine())
# df_2.index = df_2["date"]

# for i in range(1, len(df_2["return_1d"])):
#     df_2["return_1d"][i] = (df_2["return_1d"][i-1] + 1) * (df_2["return_1d"][i] + 1) - 1
#     # df_2["return_1w"][i] = (df_2["return_1w"][i - 1] + 1) * (df_2["return_1w"][i] + 1) - 1
#     # df_2["return_1m"][i] = (df_2["return_1m"][i - 1] + 1) * (df_2["return_1m"][i] + 1) - 1

# lst = [i*5 for i in range(0, len(df_2["return_1d"])/5)]
# df_w = df_2["return_1d"].iloc[lst]
# for i in range(1, len(df_w)):
#     df_w[i] = (df_w[i-1] + 1) * (df_w[i] + 1) - 1

# df = pd.DataFrame([{"i":np.nan}])
# df.to_sql("test", e.get_engine(), if_exists='append')
# print 1
#
# sql = "select * from daily_factors where date = '2017-2-7'"
# df_2 = pd.read_sql(sql, e.get_engine())
# df_2.index = df_2["code"]
# lst = df_2["divide"].dropna().order()[-20:]
#
# lst.to_csv("divide.csv")
# print lst
# print 1

# w.start()
# all_stock = w.wset("indexconstituent","date=2017-04-28;windcode=000905.SH").Data# 取全部A股股票代码、名称信息
# cyb_p = 0
# num = 0
#
# for i in range(len(all_stock[1])):
#     if all_stock[1][i][0] == '3':
#         # print all_stock[1][i]
#         cyb_p += float(all_stock[3][i])
#         num += 1
# print num, cyb_p
# print 1

# df = ts.get_h_data('002337')
total = 8
num = 8
sql_stock = "SELECT distinct(code) FROM stock.daily_k order by code"
lst = pd.read_sql(sql_stock, e.get_engine())
len_lst = len(lst['code'])
avg = len_lst / total
print total, num
sql_exist = "select distinct(code) from stock.daily_k where date='2016-11-01'"
df_code = pd.read_sql(sql_exist, e.get_engine())

for code in lst['code'][num*avg:min((num+1)*avg-1, len_lst-1)]:
    # if code[:-3] in df_code.values:
    #     continue
    time1 = datetime.now()
    # if code == '000918.SZ':
    #     print 1
    try:
        df_t = ts.get_k_data(code,start='2015-11-01', end='2016-01-01')
        df_h = ts.get_hist_data(code,start='2015-11-01', end='2016-01-01')
        df_h['date'] = df_h.index
        df = pd.merge(df_h[['turnover','date']], df_t, how='outer', on='date') # 全集避免少数据

        df.to_sql("daily_k", e.get_engine(), if_exists='append')
        # if len(df) > 0:
        #     print 1
        time2 = datetime.now()
        print code, time2-time1
    except:
        print code, 'error'