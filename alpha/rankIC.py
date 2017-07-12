# coding=utf-8
import MySQLdb
import pandas as pd
import numpy as np
import datetime
import scipy.stats

db = MySQLdb.connect(host='218.25.140.183', user='root', passwd='123456', db='stock', charset='utf8')
cursor = db.cursor()
# startdate,enddate表示因子数据的起止时间
# startdate1, enddate1 表示价格数据的起止时间
# factorname 表示因子名称
startdate = '2017-02-01'
enddate = '2017-05-31'
startdate1 = '2017-03-01'
enddate1 = '2017-06-31'


def get_date(startdate,enddate,startdate1,enddate1,months,day):
    '''
    获取因子数据日期及其对应的股票日期（daily因子和season因子）
    :param startdate: 因子起始日期
    :param enddate: 因子结束日期
    :param startdate1: 股票价格起始日期（因子起始日期下一个月）
    :param enddate1: 股票价格结束日期
    :param months: 因子数据之间共几个月
    :param day: 每月第几天
    :return: 
    '''
    sql0 = "select distinct date from daily_factors where date between '%s'and '%s' order by date" % (startdate, enddate)
    factor_date = pd.read_sql(sql0, con=db)  # 因子数据的所有日期
    sql1 = "select distinct date from daily_k where date between '%s'and '%s' order by date" % (startdate, enddate1)
    price_date = pd.read_sql(sql1, con=db)  # 价格日期的所有日期
    sqls = "select distinct date from season_factors where date between '%s'and '%s' order by date" % ( startdate, enddate)
    season_dates = pd.read_sql(sqls, con=db)
    season_dates = season_dates.values  # 获取季度因子日期
    sqls0="select distinct date from season_factors where date <'%s'order by date desc limit 1" % (season_dates[0][0].strftime('%Y-%m-%d'))
    season_report_date=pd.read_sql(sqls0,con=db).values[0][0]  # 获取初始报告日期的前一期报告日期
    season_report_dates=[]
    season_report_dates.append(season_report_date)
    for da in season_dates:
        season_report_dates.append(da[0])
    start_date = datetime.datetime.strptime(startdate, '%Y-%m-%d').date()  # 因子数据起始日期
    middle_date = start_date + datetime.timedelta(30)  # 因子数据第一个月末日期
    begin_dates = price_date[price_date['date'] < middle_date].values
    price_date = price_date[price_date['date'] >= middle_date]
    begin_date = begin_dates[0][0]
    price_start_date=price_date.values[0][0]
    end_date1=price_date.values[-1][0]
    start_date1 = datetime.datetime.strptime(startdate1, '%Y-%m-%d').date()  # 股票数据起始日期
    middle_date1 = start_date1 + datetime.timedelta(30)  # 股票数据第一个月末日期
    all_date = {}  # 月度因子日期及其对应的交易日期
    trade_days={}
    n = len(season_report_dates)
    j=0
    for i in range(months):
        factor_month = factor_date[factor_date['date'] <= middle_date].values  # 第i月因子数据所有日期
        price_month = price_date[price_date['date'] <= middle_date1].values  # 第i月股票价格数据所有日期
        factor_date = factor_date[factor_date['date'] > middle_date]  # 因子数据的其他月份日期
        price_date = price_date[price_date['date'] > middle_date1]  # 股票价格数据的其他月份日期
        if j<=(n-2):
            if (middle_date>=season_report_dates[0]) and (season_report_dates[1]-middle_date).days>5:
               factor_date1 = price_date[price_date['date'] > middle_date1].values
               trade_days[i] = [season_report_dates[0], factor_date1[day][0]]
            else:
                season_report_dates=season_report_dates[1:]
                factor_date1 = price_date[price_date['date'] > middle_date1].values
                trade_days[i] = [season_report_dates[0], factor_date1[day][0]]
                j+=1
        elif j==(n-1):
            if middle_date1<end_date1:
                factor_date1 = price_date[price_date['date'] > middle_date1].values
                trade_days[i] = [season_report_dates[0], factor_date1[day][0]]


        middle_date += datetime.timedelta(30)
        middle_date1 += datetime.timedelta(30)
        c = [factor_month[day][0], price_month[day][0]]  # 第i个月，第day天的因子数据日期和股票价格数据的日期
        all_date[i] = c
    return all_date, begin_date,trade_days,price_start_date

daily_time, daily_begin_date,season_time,season_begin_date=get_date(startdate,enddate,startdate1,enddate1,4,1)


def get_data(daily_time,season_time,daily_begin_date,season_begin_date):
    """
    
    :param daily_time: daily因子日期及对应的股票价格日期
    :param season_time: season因子日期及对应的股票价格日期
    :param daily_begin_date: daily因子起始日期的前一期月初
    :param season_begin_date: 起始日期第二个月月初
    :return: 返回每日因子数据及其对应的股票数据，季度因子数据及其对应的股票数据，市场行业种类
    """
    sql0="select code,date,close from daily_k where date= '%s'"%(daily_begin_date.strftime('%Y-%m-%d'))
    daily_price=pd.read_sql(sql0,con=db)  # 得到每日因子对应的股票本月月初数据
    sql1="select * from daily_factors where date='%s'"%(daily_time[0][0].strftime('%Y-%m-%d'))
    daily_factor=pd.read_sql(sql1,con=db) # 每日因子因子月初数据
    for i in daily_time.keys():
         sql="select date,code,close from daily_k where date='%s'"%(daily_time[i][1].strftime('%Y-%m-%d'))
         daily_p=pd.read_sql(sql,con=db)
         daily_price=pd.concat([daily_price,daily_p]) # 获取每一期的股票数据并合并
         if i!=0:   # 获取每一期的因子数据并合并
             sql_="select * from daily_factors where date='%s'"%(daily_time[i][0].strftime('%Y-%m-%d'))
             daily_f=pd.read_sql(sql_,con=db)
             daily_factor=pd.concat([daily_factor,daily_f])
    sql_s="select * from season_factors where date='%s'"%(season_time[0][0].strftime('%Y-%m-%d'))
    season_factor=pd.read_sql(sql_s,con=db)   # 获取第一期季度因子数据
    sql_s0="select date,code,close from daily_k where date='%s'"%(season_begin_date.strftime('%Y-%m-%d'))
    season_price=pd.read_sql(sql_s0,con=db)  # 获取第一期季度因子对应股票月初数据
    for j in season_time.keys():
        sqls="select date,code,close from daily_k where date='%s'"%(season_time[j][1].strftime('%Y-%m-%d'))
        season_p = pd.read_sql(sqls, con=db)
        season_price=pd.concat([season_price,season_p]) # 迭代合并季度价格数据
        if j!=0:
            sqls0="select * from season_factors where date='%s'"%(season_time[j][0].strftime('%Y-%m-%d'))
            season_f = pd.read_sql(sqls0, con=db)
            season_factor = pd.concat([season_factor, season_f])  # 迭代合并季度因子数据
    daily_price['ret']=np.nan
    daily_factor['stand_factor']=np.nan
    season_price['ret']=np.nan
    season_factor['stand_factor']=np.nan
    sqli = 'select code,industry  from stock_info'
    stock_info = pd.read_sql(sqli, con=db)  # 获取股票信息数据
    industries = stock_info['industry'].drop_duplicates().values  # 获取行业分类
    codes_info = stock_info['code'].values  # 股票信息数据的股票代码去掉后缀
    code_info = []   # 更新数据代码去掉后缀（000001.SZ转为000001）
    for cd in codes_info:
        code_info.append(cd[:-3])
    stock_info['code'] = code_info
    code_f=[]
    for c in daily_factor['code'].values:
        code_f.append(c[:-3])
    daily_factor['code']=code_f
    code_s=[]
    for d in season_factor['code'].values:
        code_s.append(d[:-3])
    season_factor['code']=code_s
    # 月度因子数据与股票信息数据合并
    daily_factor=pd.merge(daily_factor, stock_info, on=['code'], how='outer')
    season_factor=pd.merge(season_factor,stock_info,on=['code'],how='outer')  # 季度因子数据与股票信息数据合并
    daily_price.index=range(len(daily_price['code']))   # 月度因子、价格数据，季度因子、价格数据重新设置index，避免赋值时出现冲突
    daily_factor.index=range(len(daily_factor['code']))
    season_price.index=range(len(season_price['code']))
    season_factor.index=range(len(season_factor['code']))
    daily_factor=daily_factor.drop_duplicates()      # 去掉重复数据
    season_factor=season_factor.drop_duplicates()
    daily_price=daily_price.drop_duplicates()
    season_price=season_price.drop_duplicates()
    return daily_factor,daily_price,season_factor,season_price,industries

daily_factor, daily_price, season_factor, season_price, industries=get_data(daily_time,season_time,daily_begin_date,season_begin_date)

def industry_standard(all_data,factorname,industries):
    """
    
    :param all_data: 所有因子数据（daily或season）
    :param factorname: 因子名称（例如：'pe')
    :param industries: 所有行业名称
    :return: 行业中性化后的因子数据
    """
    for inds in industries:
        factor_data_ind=all_data[all_data['industry']==inds][factorname]    # 每个行业因子数据
        factor_data_ind=factor_data_ind.dropna()
        if factor_data_ind.std()!=0:
           standard_factors=(factor_data_ind-factor_data_ind.mean())/factor_data_ind.std()  # 因子中性化
           index1=standard_factors.index        # 按index对原因子数据’stand_factor列进行赋值
           for ind1 in index1:
               all_data.ix[ind1,'stand_factor']=standard_factors[ind1]
    return all_data




# 将股票价格矩阵转换为股票收益矩阵
def price2ret(pricedata,startdate):
    """
    
    :param pricedata: 股票价格数据
    :param startdate: 股票价格数据初始日期
    :return: 股票收益率数据
    """
    # merge_data=pricedata[pricedata['date']==startdate]
    # for i in time.keys():
        # price_i=pricedata[pricedata['date']==time[i][1]]
        # merge_data=pd.concat([merge_data,price_i])
    # ret_data=merge_data[merge_data['date']!=startdate]
    ret_data=pricedata[pricedata['date']!=startdate]
    codes_p=ret_data['code'].drop_duplicates()
    for code in codes_p:
        price_data=pricedata[pricedata['code']==code].sort_values('date') # 单个股票价格数据
        price_data=price_data['close'].dropna()
        df=price_data.shift(1)/price_data-1   # 求单个股票转化为收益率序列
        df=df.dropna()
        index0=df.index                       # 利用index进行赋值
        for ind0 in index0:
            ret_data.ix[ind0,'ret']=df[ind0]
    ret_data=ret_data.dropna()
    return ret_data


# 将标准化后因子矩阵与股票收益矩阵合并
# time--字典（key：第几个月，values：[第day个交易日因子数据日期，第day个交易日股票数据日期])
def data_merge(end_factor,ret_data,time,factorname,industries):
    """
    
    :param end_factor: 所有因子数据
    :param ret_data: 回报率数据
    :param time: 因子日期及对应的股票日期——dict
    :param factorname: 因子名称
    :param industries: 所有行业名称
    :return: 每个月因子数据与股票数据合并数据
    """
    merge_data={}
    for i in time.keys():
        factor_d=end_factor[end_factor['date']==time[i][0]]   # 第i期，第time[i][0]天因子数据
        factor_d1= industry_standard(factor_d,factorname,industries)  # 因子数据标准化
        price_d=ret_data[ret_data['date']==time[i][1]]        # 第i期，第time[i][1]天的股票数据
        final_data0 = pd.merge(factor_d1, price_d, on=['code'], how='inner')  # 将因子数据与股票价格数据基于股票代码进行合并
        merge_data[i]=final_data0
    return merge_data


def Rank_IC(end_data):
    """
    计算rankIC
    :param end_data: 因子与股票回报率合并数据
    :return: 每月rankIC
    """
    rank_ic={}
    for month in end_data.keys():
        relation_data=end_data[month][['stand_factor','ret']] # 获取因子数据和收益率数据
        relation_data=relation_data.dropna()
        coef=scipy.stats.spearmanr(relation_data['stand_factor'].values,relation_data['ret'].values)
        rank_ic[month]=coef[0]
    result=pd.Series(rank_ic)
    return result



factornames=['pe','pb','mktcap','beta','std','divide','moment_1m']
ret_data = price2ret(daily_price, daily_begin_date)
for factorname in factornames:
    end_data = data_merge(daily_factor, ret_data, daily_time, factorname, industries)
    rank_ic = Rank_IC(end_data)
    filename=factorname+'1.csv'
    rank_ic.to_csv(filename,encoding='utf-8')

seasonfactors=['total_rev','gross_margin','profit','eps','current','debt_asset','cash_debt','oppo_profit','roe','roa']
ret_data1 = price2ret(season_price, season_begin_date)
for season in seasonfactors:
    end_data = data_merge(season_factor, ret_data1, season_time,season, industries)
    rank_ic = Rank_IC(end_data)
    filename=season+'.csv'
    rank_ic.to_csv(filename,encoding='utf-8')











