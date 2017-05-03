import tushare as ts
import pandas as pd
from init import *

# df1 = ts.get_report_data(2015, 1)
# df2 = ts.get_report_data(2015, 2)
# print 1
e = dao()
# sql = "select code, date, close from daily_k where code='000001.SZ' and date = '2017-04-06'"
# df = pd.read_sql(sql, e.get_engine())
#
# sql_1 = "select code, date, close from daily_k where code='000002.SZ' and date = '2017-04-06'"
# df_1 = pd.read_sql(sql_1, e.get_engine())

sql_2 = "select * from factor_return where factor ='pe'"
df_2 = pd.read_sql(sql_2, e.get_engine())
df_2.index = df_2["date"]

# for i in range(1, len(df_2["return_1d"])):
#     df_2["return_1d"][i] = (df_2["return_1d"][i-1] + 1) * (df_2["return_1d"][i] + 1) - 1
#     # df_2["return_1w"][i] = (df_2["return_1w"][i - 1] + 1) * (df_2["return_1w"][i] + 1) - 1
#     # df_2["return_1m"][i] = (df_2["return_1m"][i - 1] + 1) * (df_2["return_1m"][i] + 1) - 1

lst = [i*5 for i in range(0, len(df_2["return_1d"])/5)]
df_w = df_2["return_1d"].iloc[lst]
for i in range(1, len(df_w)):
    df_w[i] = (df_w[i-1] + 1) * (df_w[i] + 1) - 1


print 1
