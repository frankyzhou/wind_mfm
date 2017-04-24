import tushare as ts
import pandas as pd
from init import *

# df1 = ts.get_report_data(2015, 1)
# df2 = ts.get_report_data(2015, 2)
# print 1
e = dao()
sql = "select code, date, close from daily_k where code='000001.SZ' and date = '2017-04-06'"
df = pd.read_sql(sql, e.get_engine())

sql_1 = "select code, date, close from daily_k where code='000002.SZ' and date = '2017-04-06'"
df_1 = pd.read_sql(sql_1, e.get_engine())
print 1
