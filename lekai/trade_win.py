# coding=utf-8

import pandas as pd

df = pd.read_excel('lekai5.xls', )
df_trade = df.ix[5:,].ix[:,[8,11,13]]
df_trade.columns = ['amount', 'money', 'fee']
df_trade.index = range(len(df_trade))
long = 0 #多头
short = 0 #空头
buy_money = 0
sell_money = 0

win = 0
lose = 0

net_lst = []


for i in range(len(df_trade)):
    data = df_trade.ix[i,:].values
    if data[1] < 0: #买入
        long += data[0]
        buy_money += data[1] - data[2] #扣取手续费
    else:           #卖出
        short += data[0]
        sell_money += data[1] - data[2]
    if long == short:
        # 净头寸为0
        if buy_money + sell_money >= 0:
            win += 1
        else:
            lose += 1
        net_lst.append(buy_money + sell_money)
        # 清零
        long = 0
        short = 0
        sell_money = 0
        buy_money = 0

net_df = pd.DataFrame(net_lst, columns=['m'])
win_m = net_df[net_df.m > 0].mean().values[0]
lose_m = net_df[net_df.m < 0].mean().values[0]
print win_m, lose_m, win_m/-lose_m
print win, lose, float(win)/(lose+win)