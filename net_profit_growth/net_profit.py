import pandas as pd
import tushare as ts
import datetime as dt
df = pd.read_excel('net_profit.xlsx').values

win_lst = []
num = 0
print len(df)
for i in df:
    num += 1
    code = i[0]
    date = i[9]
    before = date - dt.timedelta(days=10)
    try:
        k = ts.get_hist_data(code=code[:-3], start=before._date_repr, end=date._date_repr)
        if num % 100 == 0:
            print num
            win_lst.append({'code': code, 'r': k['close'].values[-1] / k['close'].values[0], 'date':date._date_repr})
    except:
        continue
df = pd.DataFrame(win_lst)
pass