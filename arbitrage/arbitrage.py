# coding=utf-8
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import statsmodels.tsa.stattools
import statsmodels.api as sm
import math
# from math import e
SIZE = 300
HANDS = 5
up_t = 2
down_t = 2


def get_gain(df, SIZE):
    dif = df.ix[:,1] * 2205 - df.ix[:,2] * df.ix[:,3]
    diff = dif.diff(1)
    mean = diff[1:].mean()
    std = diff[1:].std()
    hands_lst = []
    level_lst = []
    gain_lst = []
    hands_lst_len = []
    # max_lst = []
    # min_lst = []
    std_lst = []
    mean_lst = []

    long_short = 1  # 1代表多，-1代表空
    realized_gain = 0
    realized_gain_series = []
    gain = 1
    for i in range(len(dif)):
        if i < SIZE:
            continue
        now = dif[i]
        level = (diff[i] - mean) / std
        level_lst.append(level)
        mean_lst.append(mean)
        std_lst.append(std)
        # 判断是否止损
        if abs(level) > 2 or abs(level) < 0.5:
            # 到达历史最大值附近,清仓并设置冷却时间
            for hand in hands_lst:
                # 计算平仓利润
                if hand[0] > 0:
                    # realized_gain.append((now - hand[1]) / abs(hand[1]))
                    realized_gain += now - hand[1]
                    gain *= 1 + 0.05 * (now - hand[1]) / abs(hand[1])

                else:
                    # realized_gain.append((hand[1] - now) / abs(hand[1]))
                    realized_gain += hand[1] - now
                    gain *= 1 + 0.05 * (hand[1] - now) / abs(hand[1])
                hands_lst.pop(-1)
        elif abs(level) > 1:
            long_short = 1 if level < 0 else -1
            hands_lst.append([long_short, now])
        realized_gain_series.append(gain)

        # 浮动利润
        sum_float = 0
        if len(hands_lst) > 0:
            for i in range(len(hands_lst)):
                sum_float += hands_lst[i][1]
            if hands_lst[0][0] > 0:
                unrealized_gain = (now*len(hands_lst) - sum_float)
            else:
                unrealized_gain = (sum_float - now*len(hands_lst))
        else:
            unrealized_gain = 0

        gain_lst.append(unrealized_gain + realized_gain + 1)
        hands_lst_len.append(len(hands_lst) * long_short)
    plt.plot(gain_lst)
    # plt.show()
    plot_gain(level_lst, gain_lst, hands_lst_len, dif, std_lst, mean_lst, SIZE)


def plot_gain(level_lst, gain_lst, hands_lst, dif, std_lst, avg_lst, SIZE):
    plt.figure(1)

    plt.subplot(411) # 在图表2中创建子图1
    plt.plot(level_lst)

    plt.subplot(412) # 在图表2中创建子图2
    plt.plot(gain_lst)

    plt.subplot(413) # 在图表2中创建子图2
    plt.bar(range(len(hands_lst)), hands_lst)

    plt.subplot(414)
    plt.plot(range(len(dif.values[SIZE:])),dif.values[SIZE:], range(len(avg_lst)), avg_lst,\
             range(len(avg_lst)), np.array(avg_lst) + np.array(std_lst), \
             range(len(avg_lst)), np.array(avg_lst) - np.array(std_lst), \
             range(len(avg_lst)), np.array(avg_lst) + 2 * np.array(std_lst), \
             range(len(avg_lst)), np.array(avg_lst) - 2 * np.array(std_lst), \
             range(len(avg_lst)), np.array(avg_lst) + 3 * np.array(std_lst), \
             range(len(avg_lst)), np.array(avg_lst) - 3 * np.array(std_lst))

    plt.show()


def cal_alpha(p):
    cur_md = 0
    cur_mx = 0
    for i in range(len(p)):
        cur_mx = p[i] if p[i] > cur_mx else cur_mx
        # if p[i] > cur_mx:
        #     cur_mx = p[i]
        #     position[0] = i
        md = float(p[i] - cur_mx) / cur_mx
        cur_md = md if md < cur_md else cur_md
    return cur_md


def find_opt_size(name, start, end):
    df = pd.read_excel('arbitrage.xlsx', sheetname=name)
    kama_lst = []
    sharp_lst = []
    v = np.std(df.ix[:,2].values) * np.sqrt(240)
    for i in range(start, end):
        if i % 50 == 0:
            print i
        gain = get_gain(df, i)

        kama_lst.append(cal_alpha(gain))
        sharp_lst.append(gain[-1] / v)
    df = pd.DataFrame()
    df['kama'] = pd.Series(kama_lst, index=range(start, end))
    df['sharp']= pd.Series(sharp_lst, index=range(start, end))
    df['size'] = df.index
    df.to_csv(name+'.csv')


def judge_cointegration(df):
    # X = np.column_stack((df.ix[:,2], df.ix[:,3]))
    # model = sm.OLS(df.ix[:,1], X)
    # model = sm.OLS(df.ix[:,1].diff(1)[1:], df.ix[:,2].diff(1)[1:])
    # model = sm.OLS(np.log(df.ix[:, 1].values), np.log(df.ix[:, 2].values))
    # result = model.fit()
    # print result.summary()
    # print result.params
    # para = result.params
    # redis = df.ix[:,1][1:] - df.ix[:,2][1:] * para[0] - df.ix[:,1].diff(1)[1:] * para[1] - df.ix[:,2].diff(1)[1:] * para[2]
    # redis = df.ix[:,1].diff(1)[1:] - para[0] * df.ix[:,2].diff(1)[1:]
    # redis = df.ix[:,1] - para[0] * df.ix[:,2] - para[1] * df.ix[:,3]
    # redis = np.log(df.ix[:, 1].values) - para[0] * np.log(df.ix[:, 2].values)
    # mean = redis.mean()
    # std = redis.std()
    redis = df.ix[:,1] * 2205 - df.ix[:,2] * df.ix[:,3]
    coef = statsmodels.tsa.stattools.adfuller(redis.diff(1)[1:])
    # level_lst = []
    # for i in range(1, len(df)):
    #     redis_real = (df.ix[i,1] - df.ix[i-1,1]) - para[0] * (df.ix[i,2] - df.ix[i-1,2])
    #     level = (redis_real - mean) / std
    #     level_lst.append(level)
    # plt.plot(range(1, len(df)), level_lst)
    # plt.hist(level_lst, 100)
    # plt.show()
    return coef[1]
    # return para, mean, std


def coef_series(df, size):
    coef_lst = []
    for i in range(len(df)):
        if i < len(df) - size:
            coef_lst.append(judge_cointegration(df.ix[i:i+size]))
    print np.mean(coef_lst), np.std(coef_lst)
    plt.plot(range(len(coef_lst)), coef_lst)
    plt.show()
    pass



# find_opt_size('AG', 10, 300)
df = pd.read_excel('future.xlsx', sheetname='CU')
# dif = df.ix[:,1]
# coef = statsmodels.tsa.stattools.adfuller(dif)
# judge_cointegration(df)
get_gain(df, 60)
# coef_series(df, 100)
# cal_alpha(gain)
# plt.figure(1)
# plt.plot(gain)
# plt.show()
pass