# coding=utf-8
import pandas as pd
import datetime as dt

# 将股票交易明细表转换为估值表
data_2016_11=pd.read_excel('interview\\stock.xlsx')
# data_2016_11=pd.read_excel('chengye\\raw\\stock.xlsx')
data_2016_11=data_2016_11.dropna()
# data_2016_12=pd.read_excel('C:\\Users\\taicheng\\Desktop\\2016-12.xls').sort_values(u'业务日期')
# data_2016_11=data_2016_11.dropna()
# data_2017_1=pd.read_excel('C:\\Users\\taicheng\\Desktop\\2017-1.xls').sort_values(u'业务日期')
# data_2017_1=data_2017_1.dropna()
# data_m=pd.concat([data_2016_11,data_2016_12],ignore_index=True)
# data_m1=pd.concat([data_m,data_2017_1],ignore_index=True)
data_m1 = data_2016_11
data_m2=pd.DataFrame(data_m1.values,columns=data_m1.columns,index=range(len(data_m1)))
# 得到最终一个dataframe
# data=data_m2[[u'证券名称',u'证券代码',u'成交数量',u'委托方向',u'业务日期',u'成交金额']]
# data=pd.DataFrame(data_m2.values,columns=['name','code','deal_amount','sell_or_buy','date','deal_values']) # 取出[u'证券名称',u'证券代码',u'成交数量',u'委托方向',u'业务日期',u'成交金额']列，并转换为['name','code','deal_amount','sell_or_buy','date','deal_values']
data=pd.DataFrame(data_m2.values,columns=['date', 'code', 'deal_amount', 'deal_values', 'sell_or_buy'])
date_values = data['date'].values
for i in range(len(date_values)):
    date_values[i] = date_values[i]._date_repr

data['date'] = pd.Series(date_values, index=data.index)
# 将买卖方向‘买入’’卖出‘转换为'buy','sell'
index0=data[data['sell_or_buy']==u'买入'].index
for i in index0:
    data.ix[i]['sell_or_buy']='buy'
index1=data[data['sell_or_buy']==u'卖出'].index
for j in index1:
    data.ix[j]['sell_or_buy']='sell'

# 得到所有股票交易代码
stocks_codes=data['code'].drop_duplicates().values
# 得到交易日期
date = data['date'].drop_duplicates().sort_values().values

end_dataframe=pd.DataFrame(columns=date,index=stocks_codes)  # 建立股票持仓dataframe
base_hold=pd.DataFrame(columns=['deal_amount','sell_or_buy','deal_values'],index=stocks_codes) # 建立股票交易汇总dataframe，累计值
total={}
# 先算初始日期的占比
data_init=data[data['date']==date[0]]
values=data_init['deal_values'].sum()
ratio_stock=data_init['deal_values']/values
stocks_0=data_init['code'].drop_duplicates().values # 第一个交易日交易的股票代码
for stock in stocks_0:
    if str(stock)[0] in ['0', '3', '6']:
        df_index=data_init[data_init['code']==stock].index
        df_ratio=ratio_stock.ix[df_index].sum()   # 个股第一天持仓比
        end_dataframe.loc[stock,u'2016-11-22']=df_ratio
        base_hold.loc[stock,'deal_amount'] = sum(data_init.ix[df_index,'deal_amount'])  # 个股成交数量
        base_hold.loc[stock,'deal_values']=sum(data_init.ix[df_index, 'deal_values'])   # 个股成交金额
        base_hold.loc[stock,'sell_or_buy']=data_init.ix[df_index, 'sell_or_buy'].values[0] # 个股买卖方向
#空值全设为零
base_hold=base_hold.fillna(0)
# 将第一个交易日的数据先赋值,其余交易日数据基于第一天
total[date[0]]=base_hold
for i in range(1,len(date)):
    subdata=data[data['date']==date[i]]   # 每一天交易数据
    for code in subdata['code'].values:
        if str(code)[0] in ['0', '3', '6']:
            location=subdata[subdata['code']==code].index # 个股对应位置
            df_tmp = subdata.ix[location]
            df_sell = df_tmp[df_tmp['sell_or_buy'] == 'sell']  # 个股买卖数据
            df_buy = df_tmp[df_tmp['sell_or_buy'] == 'buy']
            if len(df_sell) > 0: # 有卖出持仓
                if base_hold.ix[code,'deal_amount']-sum(df_sell['deal_amount'].values) <=0: # 以前持仓清仓
                    if len(df_buy)==0:   # 没有买进
                        end_dataframe.ix[code, date[i]]=0
                    else: # 当天持有交易数量和交易金额等于当天的成交金额和成交数量
                        base_hold.ix[code,'deal_amount']=sum(df_buy['deal_amount'].values)
                        base_hold.ix[code, 'deal_values']=sum(df_buy['deal_values'].values)
                else: # 以前持仓有剩余
                   if len(df_buy)==0: # 没有买进，成交数量和金额等于上期持有数量和金额与本期卖出数量和金额的差值
                       base_hold.ix[code,'deal_amount']= base_hold.ix[code,'deal_amount']-sum(df_sell['deal_amount'].values)
                       base_hold.ix[code,'deal_values']= base_hold.ix[code,'deal_amount']*sum(df_sell['deal_values'].values)/sum(df_sell['deal_amount'].values) # 按当天的卖价计算当天的持有金额
                   else: # 有买进，本期持有数量等于上期数量减去本期卖出，再加上本期买入
                          # 本期持有金额等于本期买的金额+剩余持仓按今天卖价计算的金额
                       base_hold.ix[code, 'deal_values'] = sum(df_buy['deal_values'].values)+(base_hold.ix[code,'deal_amount'] - sum(df_sell['deal_amount'].values)) *sum(df_sell['deal_values'].values)/sum(df_sell['deal_amount'].values)
                       base_hold.ix[code, 'deal_amount'] = base_hold.ix[code, 'deal_amount'] - sum(df_sell['deal_amount'].values) + sum(df_buy['deal_amount'].values)

    #   没有卖
            else:
                if len(df_buy)>0:   # 当天买入，持有数量等于上期+本期，持有金额等于本期持有数量*本期成交价格
                  base_hold.ix[code, 'deal_amount'] = base_hold.ix[code, 'deal_amount'] + sum(df_buy['deal_amount'].values)
                  base_hold.ix[code, 'deal_values'] = base_hold.ix[code, 'deal_amount']*sum(df_buy['deal_values'].values)/sum(df_buy['deal_amount'].values)
    base_hold = base_hold.fillna(0)
    total[date[i]] = base_hold
    # 计算当天累计的价值
    values=sum(base_hold['deal_values'].values) # 当期持有总金额
    end_dataframe[date[i]]=base_hold['deal_values']/values  # 第i天各股票持仓比重

for time in date:
    time_str = str(time)
    # time_str = time_str[:4] + '-' + time_str[4:6] + '-' + time_str[6:8]
    time_str = time
    out_data = end_dataframe[time]
    out_data = out_data.dropna()
    insert_data = pd.DataFrame([out_data.index, out_data.values], columns=range(len(out_data)),index=['code', 'ratio']).T
    insert_data=insert_data.dropna()
    insert_data=insert_data[insert_data['ratio']!=0]  # 当天有持仓的股票数据
    insert_data['code_str']='b'
    # 将股票代码有数值型转换为字符型
    for ind in insert_data.index:
        a = str(int(insert_data.ix[ind]['code']))
        if len(a) == 6:
            insert_data.ix[ind,['code_str']] = a
        else:
            insert_data.ix[ind,['code_str']] = (6 - len(a)) * '0' + a
    filename = 'interview/' + time_str + '.xlsx'
    writer=pd.ExcelWriter(filename)
    insert_data1=insert_data[['code_str','ratio']]
    insert_data1.to_excel(writer,'Sheet1')
    writer.save






