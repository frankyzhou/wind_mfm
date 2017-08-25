# coding=utf-8
import MySQLdb
import pandas as pd
import datetime
from WindPy import w
w.start()
# get connection to database
db = MySQLdb.connect(host='218.25.140.183',user='root',passwd='123456',db='stock',charset='utf8')
cursor = db.cursor()
codes_have = []


def update_k_date():
    '''
    分已有code与新code更新
    :return: 
    '''
    date_str=datetime.datetime.today().strftime('%Y-%m-%d')
    all_stock = w.wset("SectorConstituent", u"date=" + date_str + u";sector=全部A股").Data  # get all stocks's codes
    stocklists = all_stock[1]
    sql0='select distinct code from daily_k '
    codes_in_db=pd.read_sql(sql0, con=db).values

    for code in stocklists:
        if code[:-3] in codes_in_db:
            # 记录存在的
            codes_have.append(code)
        else:
            # 不存在的直接存入
            data = w.wss(code, "ipo_date").Data
            ipo = data[0][0]
            data_no=w.wsd(code, "close,open,high,low,volume,turn", ipo, datetime.date.today(), "PriceAdj=F")
            Times_no=[]
            for i in data_no.Times:
                Times_no.append(i.date())
            data_insert_no=pd.DataFrame(data_no.Data,columns=Times_no,index=['close','open','high','low','volume','turnover']).T
            data_insert_no['code']=code[:-3]
            data_insert_no['index']=range(len(data_insert_no['code']))
            data_insert_no.index.name='date'
            pd.io.sql.to_sql(data_insert_no, 'daily_k', db, flavor='mysql', if_exists='append', index=True)
            # data_insert_no.to_sql('daily_k', db, flavor='mysql', if_exists='append', index=True)
            print code

    check_insertion_data(codes_have)


def check_insertion_data(stocklist):
    """
    检验数据库数据是否存在复权现象，并插入新的数据
    :param stocklist: 数据库已有的股票代码
    :return: 
    """
    for stock in stocklist:
        # 读取前num天的数据
        # if int(stock[:-3]) < 300062:
        #     continue
        sql = "select * from daily_k where code= %s order by date desc limit 5" % (stock[:-3])
        before_num_data = pd.read_sql(sql, con=db).sort_values('date') # 数据库旧数据
        db_data = pd.DataFrame(before_num_data[['close', 'open', 'high', 'low']].values,
                                    columns=['CLOSE', 'OPEN', 'HIGH', 'LOW'], index=before_num_data['date'])

        # wind读取num+1天数据
        data = w.wsd(stock, "close,open,high,low,volume,turn", db_data.index[0],
                     datetime.date.today(), "PriceAdj=F")
        Times = []
        for i in data.Times:
            Times.append(i.date())
        True_data = pd.DataFrame(data.Data, columns=Times, index=data.Fields).T.dropna()  # 真实数据
        change_index = []
        for a in True_data.index:
            if type(a) == unicode or str:
                a = str(a)
                change_index.append(datetime.datetime.strptime(a, '%Y-%m-%d').date())
        True_data.index = change_index

        if len(True_data.values) > 2:
            real_data = True_data[True_data.index <= db_data.index[-1]]
            real_data = real_data[['CLOSE', 'OPEN', 'HIGH', 'LOW']]

            try:
                data_concat = pd.concat([db_data, real_data], axis=1, join='inner')
                check_data = abs(data_concat.iloc[:,0].values - data_concat.iloc[:,4].values) < 0.01
            except:
                # 处理none数据
                check_data = False
                pass

            if type(check_data) == bool:
                insertion = check_data
            else:
                insertion = check_data.all()

            if insertion:
                insert1_data1 = True_data[True_data.index > db_data.index[-1]].dropna()
                if len(insert1_data1) > 0:
                    print '%s:data are same;insert new' % (stock[:-3])
                    insert1_data1.columns = ['close', 'open', 'high', 'low', 'volume', 'turnover']
                    insert1_data1['code'] = stock[:-3]
                    insert1_data1.index.name = 'date'
                    pd.io.sql.to_sql(insert1_data1, 'daily_k', db, flavor='mysql', if_exists='append', index=True)
                else:
                    print '%s:data are same;' % (stock[:-3])
            else:
                print '%s:data are different,delete and insert again' % (stock[:-3])
                cursor.execute('delete from daily_k where code=%s', stock[:-3])
                data1 = w.wsd(stock, "close,open,high,low,volume,turn", datetime.date(2015, 11, 2),
                              datetime.date.today(), "PriceAdj=F")
                Times1 = []
                for i in data1.Times:
                    Times1.append(i.date())
                insert2_data2 = pd.DataFrame(data1.Data, columns=Times1,
                                             index=['close', 'open', 'high', 'low', 'volume', 'turnover']).T.dropna()
                insert2_data2['code'] = stock[:-3]
                insert2_data2.index.name = 'date'
                pd.io.sql.to_sql(insert2_data2, 'daily_k', db, flavor='mysql', if_exists='append', index=True)


if __name__ == '__main__':
    update_k_date()







