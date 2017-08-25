# coding=utf-8
import pandas as pd
import xlrd
import re
import datetime as dt
import os
from util import *
import traceback
# name = 'daleizichan3.xlsx'
# wb = openpyxl.load_workbook(name)
# sheets = wb.get_sheet_names()
# df_dict = pd.read_excel('daleizichan3.xlsx', sheetname=sheets)


def get_data(raw):
    '''
    获得excel内容
    :param raw: 
    :return: 
    '''
    index = 0
    for i in range(len(raw)):
        if raw.ix[i, 0] == u'合约':
            index = i + 1
    # trade.columns = trade.ix[index-1,:].values
    raw.columns = range(len(raw.columns))
    raw = raw.ix[index:len(raw) - 2,:]
    return raw
    # raw.columns = ['future', 'realized profit']
    # total['value percent'] = total['value'] / sum(total['value'])
    # raw.index = raw['future']


def get_total(total):
    '''
    获得总表
    :param total: 
    :return: 
    '''
    index = 0
    for i in range(len(total)):
        if total.ix[i, 0] == u'品种':
            index = i + 1
    # trade.columns = trade.ix[index-1,:].values
    total = total.ix[index:len(total) - 2, [0, 5]]
    total.columns = ['future', 'realized profit']
    # total['value percent'] = total['value'] / sum(total['value'])
    total.index = total['future']
    return total


def judge_timedelta(time_delta):
    '''
    判断周期，当天，1-3天，4天到10天，10天以上
    :param time_delta: 
    :return: 
    '''
    if time_delta == dt.timedelta(days=0):
        return 0
    elif time_delta < dt.timedelta(days=4):
        return 1
    elif time_delta == dt.timedelta(days=10):
        return 2
    else:
        return 3


def get_margin(df):
    '''
    获得保证金序列
    :param df: 
    :return: 
    '''
    df.columns = ['future', 'factor']
    lst = df['future'].values
    for i in range(len(lst)):
        lst[i] = lst[i].split(".")[0]
    df['future'] = pd.Series(lst, index=df.index)
    df.index = df['future']
    return df


def cal_winrate(lst):
    '''
    计算每个品种的盈亏比，盈利次数，亏损次数
    :param lst: 
    :return: 
    '''
    tmp_df_1 = pd.Series(lst)
    win_df = tmp_df_1[tmp_df_1 > 0]
    lose_df = tmp_df_1[tmp_df_1 <= 0]
    win_times = len(win_df)
    lose_times = len(lose_df)
    try:
        if win_times > 0 and lose_times > 0:
            win_loss_rate = -win_df.mean() / lose_df.mean()
        elif win_times == 0:
            win_loss_rate = 0
        else:
            win_loss_rate = np.inf
    except:
        win_loss_rate = np.inf

    return [win_times, lose_times, win_loss_rate]


class cta():
    def __init__(self, name):
        '''
        提取基本信息
        :param name: 
        '''
        self.total = {}
        self.close = {}
        self.trade = {}
        self.name = name
        close_lst = []
        trade_lst = []
        net_value = {}
        self.margin_lst = get_margin(pd.read_excel('future_margin.xlsx'))
        self.margin_lst.columns = ['future','margin']
        self.cm_lst = get_margin(pd.read_excel('cm.xlsx'))
        self.cm_lst.columns = ['future', 'cm']
        for file in os.listdir(name + '/holding/'):
            if file.count('$') == 0:
                date = re.split(r'[_.]', file)[-2]
                wb = xlrd.open_workbook(name + '/holding/' + file)
                sheets = wb.sheet_names()

                self.df_dict = pd.read_excel(name + '/holding/' + file, sheetname=sheets)
                net_df = self.df_dict[u'客户交易结算日报'].fillna(0)
                self.total[date] = self.get_holding(self.df_dict[u'持仓明细'].fillna(0), get_total(self.df_dict[u'品种汇总']), net_df).fillna(0)
                self.close[date] = get_data(self.df_dict[u'平仓明细'].fillna(0))
                self.trade[date] = get_data(self.df_dict[u'成交明细'].fillna(0))
                net_value[date] = {'yes_net':float(net_df.ix[9,2]), 'today_net':float(net_df.ix[14,2]), 'today_margin':float(net_df.ix[14, 7])}

        for i in self.close.keys():
            close_lst.append(self.close[i])
            trade_lst.append(self.trade[i])
        self.all_trade = pd.concat(trade_lst)
        self.all_close = pd.concat(close_lst)
        self.all_close.index = range(len(self.all_close))
        self.add_time()
        self.net_value = pd.DataFrame(net_value).T

    def get_holding(self, holding, total, net_df):
        '''
        得到合并的持仓记录
        :param holding: 
        :param total: 
        :param net_df: 
        :return: 
        '''
        index = 0
        for i in range(len(holding)):
            if holding.ix[i, 0] == u'合约':
                index = i + 1

        holding = holding.ix[index:len(holding) - 2, [0, 2, 3, 4, 5, 8]]
        holding.columns = ['future', 'buy_amount', 'buy_price', 'sell_amount', 'sell_price', 'unrealized profit']
        for i in holding.index:
            holding['future'][i] = re.match(r'[A-Z]+', holding['future'][i]).group()
        # holding.index = holding['future']
        holding = pd.merge(holding, self.cm_lst)
        holding = pd.merge(holding, self.margin_lst)
        holding['deposit'] = (holding['buy_amount'] * holding['buy_price'] + holding['sell_amount'] * holding['sell_price']) * (holding['cm'] * holding['margin'] / 100)
        holding['long_value'] = (holding['buy_amount'] * holding['buy_price']) * (holding['cm'])
        holding['short_value'] = (holding['sell_amount'] * holding['sell_price']) * (holding['cm'])
        holding_sum = holding.groupby(['future'])['long_value', 'short_value', 'unrealized profit'].sum()
        total = pd.concat([total, holding_sum], axis=1)
        total['future'] = total.index
        total['net'] = float(net_df.ix[14, 2])
        total = total.fillna(0)
        return total

    def add_time(self):
        '''
        将时间格式转为
        :return: 
        '''
        time_dt = []
        self.all_trade.index = range(len(self.all_trade))
        for i in range(len(self.all_trade)):
            time_dt.append(dt.datetime.strptime(self.all_trade.ix[i,11] + ' ' + self.all_trade.ix[i,2], '%Y-%m-%d %H:%M:%S'))
        self.all_trade['datetime'] = pd.Series(time_dt, index=self.all_trade.index)

    def time_dist(self):
        '''
        交易（平仓，开仓）时间分布
        :return: 
        '''
        trade = self.all_trade
        times = trade.ix[:,2].values
        time_dt = []

        for t in times:
            time_dt.append(dt.datetime.strptime(t, '%H:%M:%S'))
        trade['time'] = pd.Series(time_dt, index=trade.index)
        self.all_trade = trade
        start_time = dt.datetime.strptime('1900-1-1 0:0:0', '%Y-%m-%d %H:%M:%S')
        freq_dict_open = {}
        freq_dict_close = {}
        for i in range(1, 97):
            tmp = trade[(trade.time < start_time + dt.timedelta(minutes=15*i)) & (trade.time >= start_time + dt.timedelta(minutes=15*(i-1)))]
            tmp.index = range(len(tmp))
            # tmp2 = tmp[tmp.time > start_time + dt.timedelta(minutes=15*(i-1))]
            # tmp_open = tmp[tmp.ix[:,8] == u'开'].dropna()
            num_open = 0
            num_close = 0
            for index in range(len(tmp)):
                open_close = tmp.ix[index,8].strip()
                if open_close == u'开':
                    num_open += 1
                else:
                    num_close += 1
            key = start_time + dt.timedelta(minutes=15*(i-1))
            freq_dict_open[key] = num_open
            freq_dict_close[key] = num_close
        df_open = pd.DataFrame(freq_dict_open, index=['freq']).T
        df_close = pd.DataFrame(freq_dict_close, index=['freq']).T
        df_open.to_excel(self.name+ '/data/freq_trade_open.xlsx')
        df_close.to_excel(self.name + '/data/freq_trade_close.xlsx')
        pass

    def close_freq(self):
        '''
        各品种的平仓周期分布
        :return: 
        '''
        freq_dict_win = {}
        freq_dict_lose = {}
        for i in range(len(self.all_close)):
            no = self.all_close.ix[i, 8]
            day_a = dt.datetime.strptime(self.all_close.ix[i, 9], '%Y-%m-%d')
            future = re.match(r'[A-Z]+', self.all_close.ix[i, 0]).group()
            amount = self.all_close.ix[i, 5]
            tmp_df = self.all_trade[self.all_trade.ix[:,1] == no]
            profit = self.all_close.ix[i, 7]
            if len(tmp_df) > 0:
                # if future == 'A':
                #     pass
                day_b = dt.datetime.strptime(tmp_df.ix[:,11].values[0], '%Y-%m-%d')
                time_delta = day_a - day_b
                if profit > 0:
                    if freq_dict_win.has_key(future):
                        freq_dict_win[future][judge_timedelta(time_delta)] = freq_dict_win[future][judge_timedelta(time_delta)] + 1
                    else:
                        lst = [0] * 4
                        lst[judge_timedelta(time_delta)] = 1
                        freq_dict_win[future] = lst
                else:
                    if freq_dict_lose.has_key(future):
                        freq_dict_lose[future][judge_timedelta(time_delta)] = freq_dict_lose[future][judge_timedelta(time_delta)] + 1
                    else:
                        lst = [0] * 4
                        lst[judge_timedelta(time_delta)] = 1
                        freq_dict_lose[future] = lst
        df_win = pd.DataFrame(freq_dict_win).T
        df_win.to_excel(self.name + '/data/freq_close_win.xlsx')
        df_lose = pd.DataFrame(freq_dict_lose).T
        df_lose.to_excel(self.name + '/data/freq_close_lose.xlsx')

    def margin_timeseries(self, is_normal=True):
        '''
        每日的最高最低日终保证金
        :return: 
        '''
        time_series = self.all_trade.ix[:,11].drop_duplicates()
        stats_dict = {}
        for day in time_series:
            if day not in self.net_value['today_margin'].keys():
                continue
            start_time = dt.datetime.strptime(day, '%Y-%m-%d') - dt.timedelta(hours=3)
            middle_time  = dt.datetime.strptime(day, '%Y-%m-%d')
            end_time = dt.datetime.strptime(day, '%Y-%m-%d') + dt.timedelta(hours=15)
            middle_time = end_time if is_normal else middle_time
            # 上半夜
            tmp_df = self.all_trade[(self.all_trade.datetime > start_time) & (self.all_trade.datetime < middle_time)]
            tmp_df.index = range(len(tmp_df))
            margin_end = self.net_value['today_margin'][day]
            cap_total = self.net_value['today_net'][day]
            margin_now = margin_end
            margin_max = margin_end
            margin_min = margin_end
            for i in range(len(tmp_df)-1, -1, -1):  # 负向计算
                future = re.match(r'[A-Z]+', tmp_df.ix[i, 0]).group()
                contract_value = float(tmp_df.ix[i, 7])
                open_close = tmp_df.ix[i, 8]
                try:
                    margin_rate = float(self.margin_lst[self.margin_lst.future == future].ix[:, 1].values[0]) / 100
                except:
                    pass
                margin = contract_value * margin_rate
                delta = margin if open_close == u'开' else -margin

                margin_now -= delta
                margin_max = max(margin_now, margin_max)
                margin_min = min(margin_now, margin_min)

            if not is_normal:
                # 下半夜
                tmp_df = self.all_trade[(self.all_trade.datetime > middle_time) & (self.all_trade.datetime < end_time)]
                if len(tmp_df) > 0 :
                    tmp_df.index = range(len(tmp_df))
                    margin_now = margin_end  # 重新开始计算
                    for i in range(len(tmp_df)):  # 正向计算
                        future = re.match(r'[A-Z]+', tmp_df.ix[i, 0]).group()
                        contract_value = float(tmp_df.ix[i, 7])
                        open_close = tmp_df.ix[i, 8]
                        margin_rate = float(self.margin_lst[self.margin_lst.future == future].ix[:, 1].values[0]) / 100
                        margin = contract_value * margin_rate
                        delta = margin if open_close == u'开' else -margin

                        margin_now -= delta
                        margin_max = max(margin_now, margin_max)
                        margin_min = min(margin_now, margin_min)
            margin_min = max(margin_min, 0)
            margin_end = margin_now if not is_normal else margin_end

            stats_dict[day] = {'max':margin_max, 'min':margin_min, 'close': margin_end, 'cap': cap_total}
        df = pd.DataFrame(stats_dict).T
        df.to_excel(self.name + "/data/margin_series.xlsx")

    def export_total(self):
        '''
        输出每天的保证金占比详情
        :return: 
        '''
        for name in self.total.keys():
            self.total[name].to_excel(self.name + '/data/margin_percent/magrin_percent_' + name + '.xlsx')

    def cal_winrate_and_lossrate(self):
        '''
        计算df中胜率和盈亏比
        胜率：盈利次数 / 总次数
        盈利比：盈利率/亏损率
        盈利率：盈利/保证金
        :return: 
        '''
        freq_dict = {}
        stats_dict = {}
        for i in range(len(self.all_close)):
            no = self.all_close.ix[i, 8]
            day_a = dt.datetime.strptime(self.all_close.ix[i, 9], '%Y-%m-%d')
            future = re.match(r'[A-Z]+', self.all_close.ix[i, 0]).group()
            amount = self.all_close.ix[i, 5]
            profit = self.all_close.ix[i, 7]
            tmp_df = self.all_trade[self.all_trade.ix[:, 1] == no]
            if len(tmp_df) > 0:
                day_b = dt.datetime.strptime(tmp_df.ix[:, 11].values[0], '%Y-%m-%d')
                time_delta = day_a - day_b
                all_amount = tmp_df.ix[:, 6].values[0]
                all_nominal_value = tmp_df.ix[:, 7].values[0]
                margin_rate = float(self.margin_lst[self.margin_lst.future == future].ix[:, 1].values[0]) / 100
                margin = all_nominal_value * float(amount) / all_amount * margin_rate
                profit_rate = profit / margin
                if freq_dict.has_key(future):
                    freq_dict[future][judge_timedelta(time_delta)].append(profit_rate)
                else:
                    lst = [[], [], [], []]
                    lst[judge_timedelta(time_delta)].append(profit_rate)
                    freq_dict[future] = lst

        all_win = []

        for f in freq_dict.keys():
            lst = freq_dict[f]
            tmp_stat = []
            tmp_lst = []
            for i in range(len(lst)):
                tmp_stat.extend(cal_winrate(lst[i]))
                tmp_lst.extend(lst[i])
            all_win.extend(tmp_lst)
            tmp_stat.extend(cal_winrate(tmp_lst))
            stats_dict[f] = tmp_stat
        df = pd.DataFrame(stats_dict).T
        df.to_excel(self.name + '/data/winrate.xlsx')
        print cal_winrate(all_win)

    def cal_profit_all_by_future(self):
        '''
        统计每个品种的盈利汇总
        :return: 
        '''
        stats_dict = {}
        for i in range(len(self.all_close)):
            no = self.all_close.ix[i, 8]
            future = re.match(r'[A-Z]+', self.all_close.ix[i, 0]).group()
            amount = self.all_close.ix[i, 5]
            profit = self.all_close.ix[i, 7]
            tmp_df = self.all_trade[self.all_trade.ix[:, 1] == no]
            if len(tmp_df) > 0:
                all_amount = tmp_df.ix[:, 6].values[0]
                all_nominal_value = tmp_df.ix[:, 7].values[0]
                margin_rate = float(self.margin_lst[self.margin_lst.future == future].ix[:, 1].values[0]) / 100
                margin =  all_nominal_value * float(amount) / all_amount * margin_rate
                if stats_dict.has_key(future):
                    stats_dict[future]['margin'] = stats_dict[future]['margin'] + margin
                    stats_dict[future]['profit'] = stats_dict[future]['profit'] + profit
                else:
                    stats_dict[future] = {}
                    stats_dict[future]['margin'] = margin
                    stats_dict[future]['profit'] = profit

        df = pd.DataFrame(stats_dict).T
        df.to_excel(self.name + '/data/profit.xlsx')

    def get_future_name_series(self):
        '''
        统计每日交易的期货品种个数
        :return: 
        '''
        times_dict = {}
        for t in self.trade.keys():
            tmp_df = self.trade[t]
            codes = tmp_df.ix[:,0].values
            set_codes = set()
            for c in codes:
                set_codes.add(re.match(r'[A-Z]+', c).group())
            times_dict[t] = len(set_codes)
        df = pd.DataFrame(times_dict, index=[1]).T
        df.to_excel(self.name + '/data/future_num.xlsx')
        pass

    def get_pair_trade(self):
        '''
        获得每天配对交易情况
        :return: 
        '''
        for day in self.trade.keys():
            stats_dict = {}
            tmp_df = self.trade[day]
            time_series = tmp_df.ix[:,2].drop_duplicates().values
            for t in time_series:
                tmp_df_1 = tmp_df[tmp_df.ix[:,2] == t].dropna()
                codes = tmp_df_1.ix[:,0].drop_duplicates().values
                code_hands = {}
                if len(codes) > 1:
                    for code in codes:
                        tmp_code = tmp_df_1[tmp_df_1.ix[:,0] == code]
                        tmp_open = tmp_code[tmp_code.ix[:,8] == u'开'].groupby(tmp_code.columns[0]).sum()
                        tmp_close = tmp_code[tmp_code.ix[:,8] != u'开'].groupby(tmp_code.columns[0]).sum()
                        num_open = tmp_open.ix[:,6].values[0] if len(tmp_open) > 0 else 0
                        num_close = tmp_close.ix[:, 6].values[0] if len(tmp_close) > 0 else 0
                        code_hands[code] = num_open - num_close
                    stats_dict[day + ' ' + t] = code_hands
            df = pd.DataFrame(stats_dict).T.fillna(0)
            if len(df) > 0:
                df.to_excel(self.name + "/data/pair_trade/pair_trade_" + day + ".xlsx")

    def cal_gap_series(self, is_normal=True):
        '''
        获得
        :param is_normal: 
        :return: 
        '''
        time_series = self.all_trade.ix[:, 11].drop_duplicates()
        stats_dict = {}
        for day in time_series:
            if day not in self.net_value['today_margin'].keys():
                continue
            start_time = dt.datetime.strptime(day, '%Y-%m-%d') - dt.timedelta(hours=3)
            middle_time = dt.datetime.strptime(day, '%Y-%m-%d')
            end_time = dt.datetime.strptime(day, '%Y-%m-%d') + dt.timedelta(hours=15)
            # 上半夜
            middle_time = end_time if is_normal else middle_time
            tmp_df = self.all_trade[(self.all_trade.datetime > start_time) * (self.all_trade.datetime < middle_time)]
            tmp_df.index = range(len(tmp_df))
            total_df = self.total[day]
            short_value_end = total_df['short_value'].sum()
            long_value_end = total_df['long_value'].sum()
            short_value = short_value_end
            long_value = long_value_end
            gap_max = long_value - short_value
            for i in range(len(tmp_df)-1, -1, -1):
                trade = tmp_df.ix[i, 3].strip()
                contract_value = float(tmp_df.ix[i, 7])
                open_close = tmp_df.ix[i, 8]
                contract_value = contract_value if open_close == u'开' else -contract_value

                if trade == u'买':
                    long_value -= contract_value
                else:
                    short_value -= contract_value

                if abs(gap_max) < abs(long_value - short_value):
                    gap_max = long_value - short_value

            if not is_normal:
                # 下半夜
                tmp_df = self.all_trade[(self.all_trade.datetime > middle_time) & (self.all_trade.datetime < end_time)]
                if len(tmp_df) > 0 :
                    tmp_df.index = range(len(tmp_df))
                    short_value = short_value_end
                    long_value = long_value_end
                    gap_max = long_value - short_value
                    for i in range(len(tmp_df)):
                        trade = tmp_df.ix[i, 3].strip()
                        contract_value = float(tmp_df.ix[i, 7])
                        open_close = tmp_df.ix[i, 8]
                        contract_value = contract_value if open_close == u'开' else -contract_value

                        if trade == u'买':
                            long_value -= contract_value
                        else:
                            short_value -= contract_value

                        if abs(gap_max) < abs(long_value - short_value):
                            gap_max = long_value - short_value

            gap_end = long_value_end-short_value_end if is_normal else long_value - short_value
            stats_dict[day] = {'max':gap_max, 'end':gap_end}

        df = pd.DataFrame(stats_dict).T

        df.to_excel(self.name + '/data/gap_series.xlsx')

    def cal_cost_to_profit(self):
        '''
        计算总的盈利和手续费
        计算总的空头盈利和多头盈利
        :return: 
        '''
        total_profit = 0
        long_profit = 0
        short_profit = 0
        cost = self.all_trade.ix[:,9].sum()

        for i in self.all_trade.index:
            try:
                trade_direction = self.all_trade.ix[i, 3].strip()
                profit = self.all_trade.ix[i, 10]
                total_profit += float(profit)
                if trade_direction == u'买':
                    short_profit += float(profit)
                else:
                    long_profit += float(profit)
            except:
                continue
        cost_dict = {}
        cost_dict['cost'] = cost
        cost_dict['profit'] = total_profit
        cost_dict['long_profit'] = long_profit
        cost_dict['short_profit'] = short_profit
        df = pd.DataFrame(cost_dict, index=[1]).T
        df.to_excel(self.name + '/data/cost_profit.xlsx')

    def profit_details_by_future(self):
        '''
        不同品种的盈利率详情
        盈利率 = （平仓价格 - 开仓价格） / 开仓价格
        :return: 
        '''
        stats_dict = {}
        df_lst = []
        for i in self.all_close.index:
            future = re.match(r'[A-Z]+', self.all_close.ix[i, 0]).group()
            trade_direction = self.all_close.ix[i, 2].strip()
            profit_rate = (self.all_close.ix[i, 3] - self.all_close.ix[i, 4]) / float(self.all_close.ix[i, 4])
            profit_rate = - profit_rate if trade_direction == u'买' else profit_rate
            if stats_dict.has_key(future):
                stats_dict[future].append(profit_rate)
            else:
                lst = [profit_rate]
                stats_dict[future] = lst
        for k in stats_dict.keys():
            df_lst.append(pd.DataFrame(stats_dict[k], columns=[k]))
        df = pd.concat(df_lst, axis=1)
        df.to_excel(self.name + '/data/profit_rate.xlsx')

    def cal_trade_hands(self):
        freq_dict_win = {}
        freq_dict_lose = {}
        for i in range(len(self.all_close)):
            no = self.all_close.ix[i, 8]
            day_a = dt.datetime.strptime(self.all_close.ix[i, 9], '%Y-%m-%d')
            future = re.match(r'[A-Z]+', self.all_close.ix[i, 0]).group()
            amount = self.all_close.ix[i, 5]
            tmp_df = self.all_trade[self.all_trade.ix[:, 1] == no]
            profit = self.all_close.ix[i, 7]
            if len(tmp_df) > 0:
                # if future == 'A':
                #     pass
                day_b = dt.datetime.strptime(tmp_df.ix[:, 11].values[0], '%Y-%m-%d')
                time_delta = day_a - day_b
                if profit > 0:
                    if freq_dict_win.has_key(future):
                        freq_dict_win[future][judge_timedelta(time_delta)] = freq_dict_win[future][
                                                                                 judge_timedelta(time_delta)] + amount
                    else:
                        lst = [0] * 4
                        lst[judge_timedelta(time_delta)] = 1
                        freq_dict_win[future] = lst
                else:
                    if freq_dict_lose.has_key(future):
                        freq_dict_lose[future][judge_timedelta(time_delta)] = freq_dict_lose[future][
                                                                                  judge_timedelta(time_delta)] + amount
                    else:
                        lst = [0] * 4
                        lst[judge_timedelta(time_delta)] = 1
                        freq_dict_lose[future] = lst
        df_win = pd.DataFrame(freq_dict_win).T
        df_win.to_excel(self.name + '/data/hand_close_win.xlsx')
        df_lose = pd.DataFrame(freq_dict_lose).T
        df_lose.to_excel(self.name + '/data/hand_close_lose.xlsx')

cta = cta('./allstar')
cta.time_dist()  # 下单时间分布
cta.close_freq()  # 平仓频率分布
cta.margin_timeseries(True)
cta.export_total()  # 输出所有数据
cta.cal_winrate_and_lossrate()  # 计算盈亏比与胜率
cta.cal_profit_all_by_future()  # 计算每个品种盈利
cta.get_future_name_series()  # 得到交易品种个数序列
cta.get_pair_trade()  # 得到配对交易
cta.cal_gap_series(True)  # 计算每日轧差序列
cta.cal_cost_to_profit()  # 计算收益所需成本
cta.profit_details_by_future()  # 品种盈利数据统计
cta.cal_trade_hands()  # 计算品种交易手数
pass