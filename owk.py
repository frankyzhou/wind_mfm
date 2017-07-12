# -*- coding: utf-8 -*-
"""
Created on Mon Mar 06
"""

import MySQLdb
import pandas as pd
from copy import deepcopy

info_ = {'host': '218.25.140.183', 'user': 'root', 'passwd': '123456', 'db': 'mom', 'charset': "utf8"}
# id_list = ['HF0000000A','HF0000159R','HF000028HD','HF000028I0','HF000028J0','HF000028JM','HF000028N3',
#           'HF000028N4','HF000028N5','HF000028N6','HF000028NB','HF0000160B','HF000028OM','HF000028RA',
#           'HF000028RI','HF000028RJ','HF000028S2','HF000028UX']

input_data = {'end_date': '2017-02-05', 'period': '1-Year', 'persent': '0.025'}
# period = input_data['period']

# end_date = input_data['end_date']
# persentt  = float(input_data['persent'])

host_name = info_['host']
user_name = info_['user']
pass_wd = info_['passwd']
data_b = info_['db']
char = info_['charset']

period = input_data['period']
end_date = input_data['end_date']

try:
    conn = MySQLdb.connect(host=host_name, user=user_name, passwd=pass_wd, db=data_b, charset=char)
except MySQLdb.debug as e:
    print e

period = input_data['period']
end_date = input_data['end_date']
# date1 = get_rite_date(end_date) //注意
persentt = float(input_data['persent'])

sql = 'SELECT * FROM index_records WHERE enddate =' + '"' + str(end_date) + '"' + 'AND period =' + '"' + str(
    period) + '"'
df = pd.read_sql(sql, conn)
del df['index'], df['enddate'], df['period']
# d_new = df.sort_values(by = 'Cumulative_yield')
# d_new = d_new.reset_index(drop=True)
# del df['ID']

lent = int(len(df) * persentt)
columns_names = ['Cumulative_yield', 'Annualized_yield', 'Annualized_volatility',
                 'Average_yield', 'Worst_yield', 'Best_yield', 'Average_positive',
                 'Average_negative', 'Annualized_downsiderisk_0',
                 'Annualized_downsiderisk_average', 'skewness', 'kurtosis',
                 'Cumulative_profitloss', 'VaR_95', 'VaR_99', 'CFVaR_99', 'ETL 99',
                 'maxdd', 'sharpe', 'Sortino', 'Calmar']
columns_names = ['Cumulative_yield']
# df['Annualized_volatility'] = df
df.index = df['ID']
del df['ID']

columns_verse_names = ['Annualized_volatility','Annualized_downsiderisk_0',
                 'Annualized_downsiderisk_average','kurtosis','maxdd']
# for i in columns_verse_names:
#     df[i] = -df[i]
w = 0.025
lst_df = []

for j in columns_names:
    if j in columns_verse_names:
        sort = df[j].order(ascending=True, na_position="last")
    sort = df[j].order(ascending=False, na_position="first")
    l = len(sort)
    # for x in range(0, int(w*l)-1):
    sort[0:int(w*l)-1] = 1
# for y in range():
    sort[int((1-w)*l)-1:l] = 100
    for i in range(int(w*l), int((1-w)*l)):
        sort[i] = int((i - w*l) * 100 / ((1-2*w)*l)) + 1
    df_tmp = pd.DataFrame(sort)
    df_tmp.columns = ["w"]
    lst_df.append(df_tmp)

df_sum = deepcopy(lst_df[0])
for i in range(1, len(lst_df)):
    df_sum += lst_df[i]
df_sum = df_sum / len(columns_names)

lst_result = []
lst_result.append(df_sum.index.values)
lst_result.append(df_sum["w"].values)
pass

