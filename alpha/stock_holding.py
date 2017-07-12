# coding=utf-8
import xlrd
from numpy import *
from sklearn import linear_model

from alpha.getFactor import *

reg=linear_model.LinearRegression()

w.start()
factors_not_industry = ['mktcap', 'divide', 'current', 'debt_asset', 'close', 'turn']
factors_d = ['pe', 'pb', 'divide', 'mktcap', 'beta', 'std']
factors_barra = ['value', 'debt', 'moment_1m', 'turnover', 'mktcap', 'std', 'beta', 'growth']

def open_excel(file= 'file.xls'):
    try:
        data = xlrd.open_workbook(file)
        return data
    except Exception,e:
        print str(e)


#根据索引获取Excel表格中的数据   参数:file：Excel文件路径     colnameindex：表头列名所在行的所以  ，by_index：表的索引
def excel_table_byindex(file,colnameindex,by_index=0):
    data = open_excel(file)
    table = data.sheets()[by_index]
    nrows = table.nrows #行数
    ncols = table.ncols #列数
    colnames =  table.row_values(colnameindex) #某一行数据
    list =[]
    for rownum in range(1,nrows):
         row = table.row_values(rownum)
         if row:
             app = {}
             for i in range(len(colnames)):
                app[colnames[i]] = row[i]
             list.append(app)
    return list


def get_index_stocks(index, date):
    all_stock = w.wset("indexconstituent","date=%s;windcode=%s" %(date, index)).Data
    stock_lst = []
    for i in range(len(all_stock[1])):
        stock = {}
        stock["code"] = all_stock[1][i]
        stock["percent"] = all_stock[3][i]/100 if all_stock[3][i] != None else 0
        stock_lst.append(stock)
    df = pd.DataFrame(stock_lst)
    df.index = df['code']
    return df


def get_stock_holding(name):
    stock_lst = []
    code = u'证券代码'
    percent = u'持仓市值(净价)/单元净值(%)'
    tables = excel_table_byindex(name+'.xls', 0)
    sum_percent = 0
    for row in tables:
        # if row[code][:4] == '1102' and len(row[code]) > 10:
        #     stock = {}
        #     stock_code = row[code][-6:]
        #     maker = "H" if stock_code[0] == '6' else "Z"
        #     stock["code"] = stock_code + ".S" + maker
        #     stock["percent"] = row[percent]
        #     sum_percent += row[percent]
        #     stock_lst.append(stock)
        stock = {}
        stock_code = row[code]
        if len(stock_code) == 6 and stock_code[0] in ['0','3','6']:
            maker = "H" if stock_code[0] == '6' else "Z"
            stock["code"] = stock_code + ".S" + maker
            stock["percent"] = row[percent]
            sum_percent += row[percent]
            stock_lst.append(stock)
    if sum_percent > 0:
        for stock in stock_lst:
            stock["percent"] = stock["percent"] / sum_percent
    df = pd.DataFrame(stock_lst)
    if len(df) > 0:
        df.index = df['code']
    return df


def get_stock_holding_pandas(name):
    df_raw = pd.read_excel(name+'.xlsx')
    df_raw.columns = ['code', 'percent']
    df = get_codemaker_df(df_raw)
    df.index = df['code']

    return df[df.code!='511990.SZ']


def get_cap_percent(df, stock_df):
    cap_pct = [0] * 3
    try:
        if len(stock_df) > 0:
            for code in stock_df['code'].values:
                cap = df[df.code == code]["mktcap"].values[0] / 100000000
                if cap < 100:
                    cap_pct[0] += stock_df["percent"][code]
                elif cap < 500 and cap > 100:
                    cap_pct[1] += stock_df["percent"][code]
                else:
                    cap_pct[2] += stock_df["percent"][code]
            sum_percent = sum(cap_pct)
            if sum_percent > 0:
                for i in range(len(cap_pct)):
                    cap_pct[i] = cap_pct[i] / sum_percent
    except:
        traceback.print_exc()
        pass
    return cap_pct


def get_indu_percent(df, stock_df):
    industry_set = set()
    sum_lst = 0
    for i in df["industry"].values:
        if i != "None":
            industry_set.add(i)
    industry_lst = list(industry_set)
    # industry_lst = df["industry"].drop_duplicates().values
    industry_pct = [0] * len(industry_lst)
    if len(stock_df) > 0:
        for code in stock_df['code'].values:
            industry = df[df.code == code]["industry"].values[0]
            i = industry_lst.index(industry)
            industry_pct[i] += stock_df["percent"][code]

    sum_lst = sum(industry_pct)
    if sum_lst > 0:
        for i in range(len(industry_pct)):
            industry_pct[i] = industry_pct[i] / sum_lst
    return industry_pct, industry_lst


def find_index_rank(index, code):
    for i in range(len(index)):
        if index[i] == code:
            return float(i)/len(index)
    return 0.5


def get_factor_return_in_time(startdate, enddate):
    trade_days = ts.get_k_data("000001", startdate, enddate)["date"].values
    for date_str in trade_days:
        # if os.path.exists(dir_name +'/data/return/fact_return_'+date_str+'.csv'):
        #     continue
        print date_str
        fact_return = s_h.get_factor_return(date_str)
        fact_return.to_csv(dir_name +'/data/return/fact_return_'+date_str+'.csv',encoding='utf-8')


def get_stock_cov_in_time(startdate, enddate):
    trade_days = ts.get_k_data("000001", startdate, enddate)["date"].values
    for date_str in trade_days:
        if os.path.exists(dir_name +'/data/stock_cov/stock_cov_' + date_str + '.csv'):
            continue
        print date_str
        stock_cov = s_h.get_stock_cov(date_str, 10)
        stock_cov.to_csv(dir_name +'/data/stock_cov/stock_cov_' + date_str + '.csv', encoding='utf-8')


def get_factor_cov_in_time(startdate, enddate):
    trade_days = ts.get_k_data("000001", startdate, enddate)["date"].values
    factor_lst = []
    for date_str in trade_days:
        if os.path.exists(dir_name +'/data/fact_cov/fact_cov_' + date_str + '.csv'):
            continue
        print date_str
        factor_df = pd.read_csv(dir_name +'/data/return/fact_return_' + date_str + '.csv', index_col=0)
        factor_df.index = [date_str]
        factor_lst.append(factor_df)
    df_all = pd.concat(factor_lst).T
    for i in range(len(trade_days)-5):
        cov = np.cov(df_all.iloc[:,i:i+5])
        df_cov = pd.DataFrame(cov, index=df_all.index, columns=df_all.index)
        df_cov.to_csv(dir_name +'/data/fact_cov/fact_cov_' + str(df_all.columns[i]) + '.csv')


def cal_df_dif(filePath, outPath):
    df_lst = []
    # index = []
    for i in os.listdir(filePath):
        # if 'fact' in i:
        if i[-1] == 'x':
            continue
        try:
            df = pd.read_csv(filePath + "/" + i,  index_col=0).T # index_col=0获得index
            # else:
            #     df = pd.read_csv(filePath + "/" + i,  index_col=0)
            df[i.split('_')[2][:-4]] = df['p'] - df['b']
            df_lst.append(df.iloc[:,2:3])
        except:
            pass
    df_all = pd.concat(df_lst, axis=1).T
    df_all.to_csv(outPath, encoding='gbk')


def get_stock_risk(date_str, stock_df, filePathIn, filePathOut=None):
    code_lst = []
    if len(stock_df) == 0:
        return 0
    for c in stock_df['code'].values:
        code_lst.append(long(c[:6]))
    # stock_df['code'] = pd.Series(code_lst, index=stock_df.index)
    stock_df.index = code_lst
    df_cov = pd.read_csv(filePathIn + date_str + '.csv', index_col=0)
    df_cov.columns = df_cov.index
    # df_cov = stock_df['percent']
    df_1 = pd.concat([df_cov, stock_df], axis=1)
    df_p = df_1['percent'].fillna(0)
    del df_1['code']
    del df_1['percent']
    df_2 = pd.concat([df_1, stock_df.T], axis=0)
    df_2 = df_2.drop(df_2.index[-2:])
    df_2 = df_2.fillna(0)


    risk_margin = np.dot(df_2.values, df_p.values)
    total_risk = np.dot(risk_margin, df_p.values)
    if not filePathOut:
        return total_risk
    risk_margin /= total_risk
    risk_df = pd.DataFrame(risk_margin, index=df_p.index, columns=['risk'])

    risk_df_sort = risk_df.sort_values('risk', ascending=False)
    risk_df_rank = pd.DataFrame([float(i)/len(risk_df_sort) for i in range(0,len(risk_df_sort)) ], index=risk_df_sort.index, columns=['r'])

    risk_df_port = pd.concat([ stock_df, risk_df, risk_df_rank], join='inner', axis=1)
    risk_df_port.index= risk_df_port['code']
    del risk_df_port['code']
    risk_df_port.to_excel(filePathOut + date_str + '.xlsx')


def get_track_error(stock_df, bench_df, date_str, filePathIn):
    if len(stock_df) > 0:
        df_all = pd.concat([stock_df, bench_df], join='outer', axis=1).fillna(0)
        df_all.ix[:,1] = df_all.ix[:,1] - df_all.ix[:,3]
        df_all['code'] = df_all.index
        df_all = df_all.ix[:,:2]
    else:
        bench_df.ix[:,1] = - bench_df.ix[:,1]
        df_all = bench_df
    return get_stock_risk(date_str, df_all, filePathIn)


def get_fact_risk(date_str, fact_dif_path, fact_cov_path, file_out):
    fact_df = pd.read_csv(fact_dif_path +  date_str + '.csv', index_col=0)
    p = fact_df.ix['p']
    dif = fact_df.ix['p'] - fact_df.ix['b']
    fact_cov = pd.read_csv(fact_cov_path +  date_str + '.csv', index_col=0)

    dot_p = np.dot(fact_cov.values, p.values)
    dot_p_all = np.dot(dot_p, p.values)
    dot_p = dot_p / dot_p_all if dot_p_all != 0 else dot_p
    df_p = pd.DataFrame(dot_p, index=fact_df.columns, columns=['mctr'])

    dot_dif = np.dot(fact_cov.values, dif.values)
    dot_dif_all = np.dot(dot_dif, dif.values)
    dot_dif = dot_dif / dot_dif_all if dot_dif_all != 0 else dot_dif
    df_dif = pd.DataFrame(dot_dif, index=fact_df.columns, columns=['mcar'])

    df_all = pd.concat([df_p, df_dif], join='outer', axis=1)
    df_all.to_excel(file_out + date_str + ".xlsx")
    # pass


def get_fact_alpha(return_path, factor_path, filePathOut):
    file_lst = os.listdir(return_path)
    df_lst = []
    name_lst = []
    for name in file_lst:
        df_return = pd.read_csv(return_path + '/' + name, index_col=0)
        df_lst.append(df_return)
        name_lst.append(name.split('_')[2][:-4])
    df_return_all = pd.concat(df_lst)
    df_return_all.index = name_lst
    df_factor = pd.read_csv(factor_path, index_col=0)
    df_return_all = df_return_all.ix[df_factor.index]
    df_fact_alpha = df_return_all * df_factor
    df_fact_alpha.to_excel(filePathOut)


def get_excess_stock(stock_df, bench_df):
    df_all = pd.DataFrame()
    if len(stock_df) > 0:
        df_all = pd.concat([stock_df, bench_df], join='outer', axis=1).fillna(0)
        df_all = df_all.ix[stock_df.index]
        df_all.ix[:,1] = df_all.ix[:,1] - df_all.ix[:,3]
        df_all['code'] = df_all.index
        df_all = df_all.ix[:,:2]
    return df_all


class stock_holding():
    def __init__(self):
        self.db = dao()
        self.engine = self.db.get_engine()
        self.cal = cal_return()
        self.trade_days = ts.get_k_data("000001", '2015-01-01', '2017-01-01')["date"].values

    def get_cap_percent(self, stock_lst, bench_stocklst, date_str):
        '''
        获取投顾在某个时期的大小盘敞口
        :param stock_lst:
        :param bench_stocklst:
        :param date_str:
        :return:
        '''
        df_dict = {}

        sql = "select code, mktcap from daily_factors where date='%s'" % date_str
        df = pd.read_sql(sql, self.engine)

        df_dict["p"] = get_cap_percent(df, stock_lst)
        df_dict["b"] = get_cap_percent(df, bench_stocklst)


        df_cap = pd.DataFrame(df_dict)
        df_cap.index = ["小盘股", "中盘股", "大盘股"]
        # plt.figure()
        # df_cap.plot.bar()
        # plt.savefig("cap_pct.png")
        return df_cap.T

    def get_inds_percent(self, stock_lst, bench_stocklst):
        '''
        获取投顾在不同行业的敞口
        :param stock_lst:
        :param bench_stocklst:
        :return:
        '''
        df_dict = {}

        sql = "select code, industry from stock_info"
        df = pd.read_sql(sql, self.engine)

        df_dict["p"], indu_lst = get_indu_percent(df, stock_lst)
        df_dict["b"], indu_lst = get_indu_percent(df, bench_stocklst)

        df = pd.DataFrame(df_dict, index=indu_lst)
        # df.plot.bar(figsize=[15,8])
        # plt.savefig("indu_pct.png")
        return df.T

    def get_factor_load(self, date_str):
        stock_df = self.cal.get_stock(date_str, "d").drop_duplicates()
        stock_df = self.merge_industry(stock_df)
        # stock_df.index = stock_df['code']

        stock_df_s = self.cal.get_stock(date_str, "s").drop_duplicates()
        stock_df_s = self.merge_industry(stock_df_s)
        # stock_df_s.index = stock_df_s['code']

        stock_df_k = self.cal.get_stock(date_str, "k").drop_duplicates()
        stock_df_k = get_codemaker_df(stock_df_k)
        stock_df_k = self.merge_industry(stock_df_k)
        # stock_df_k.index = stock_df_k['code']

        df = pd.merge(stock_df, stock_df_s, how='outer', on='code')
        df = pd.merge(df, stock_df_k[['code', 'turnover']], how='outer', on='code')
        df = df.fillna(0)
        del df['date_y']
        del df['date_x']
        # del df['industry_y']
        df['debt'] = (df['current'] + df['debt_asset']) / 2
        df['value'] = (df['pe'] + df['pb'] + df['divide']) / 3
        df['growth'] = (df['total_rev_g'] + df['gross_margin_g'] + df['oppo_profit_g'] + df['profit_g']) / 4

        industry_set = list(df['industry_x'].drop_duplicates().values)
        df_lst = []
        for i in industry_set:
            if i == 'None' or i == 0:
                industry_set.remove(i)
                continue
            df_tmp = df[df.industry_x==i]
            df_tmp[i] = 1
            del df_tmp['industry_x']
            df_lst.append(df_tmp)
        df_all = pd.concat(df_lst).fillna(0)
        columns = ['code', 'value', 'debt', 'moment_1m', 'turnover', 'mktcap', 'std', 'beta', 'growth']
        # columns.extend(industry_set)
        # for c in columns:
        #     if c not in df_all.columns:
        #         print c
        return df_all[columns]

    def get_factor_percent(self, stock_df, date_str):
        # df_new = df[['industry_x', 'value', 'debt', 'moment_1m', 'turnover', 'mktcap', 'std', 'beta', 'growth']]
        df = self.get_factor_load(date_str)
        factor_load = {}
        if len(stock_df) > 0:
            df_join = pd.merge(df, stock_df, how='inner', on='code')
        for f in factors_barra:
            factor_load[f] = sum((df_join[f] * df_join['percent'])) if len(stock_df) > 0 else 0
        return pd.DataFrame([factor_load])

    def merge_industry(self, stock_df):
        sql_getindustry = "select code, industry from stock_info"
        df_code = pd.read_sql(sql_getindustry, self.engine)
        df_all = pd.merge(df_code, stock_df, on='code')  # 通过merge剔除新股
        df_all = self.cal.industry_factor(df_all)  # 行业中性化

        # industry_set = df_code['industry'].drop_duplicates().values
        # df_lst = []
        # for i in industry_set:
        #     if i == 'None':
        #         continue
        #     df_tmp = df_all[df_all.industry==i]
        #     df_tmp[i] = 1
        #     del df_tmp['industry']
        #     df_lst.append(df_tmp)
        # df_all = pd.concat(df_lst).fillna(0)
        return df_all

    def get_stock_return(self, date_str):
        sql_k = "select code, close from daily_k where date = '%s' order by code" % (date_str)
        df_k = pd.read_sql(sql_k, self.engine)

        next_day = find_tradeday(date_str, self.trade_days, 20)
        sql_k20 = "select code, close from daily_k where date = '%s' order by code" % (next_day)
        df_k20 = pd.read_sql(sql_k20, self.engine)

        df = pd.merge(df_k, df_k20, how='outer', on='code')
        df['return'] = df['close_y'] / df['close_x']
        df = get_codemaker_df(df.fillna(1))
        return df[['code', 'return']]

    def get_factor_return(self, date_str):
        stock_return = self.get_stock_return(date_str)
        factor_load = self.get_factor_load(date_str)
        df_all = pd.merge(stock_return, factor_load, how='inner', on='code')
        r_np = df_all.ix[:,1].values  # return
        f_np = df_all.ix[:,2:].values
        reg.fit(f_np, r_np)
        factor_return = pd.DataFrame(reg.coef_, index=df_all.columns[2:])
        mean = factor_return.mean()
        std = factor_return.std()
        return factor_return.T

    def get_stock_cov(self, date_str, size):
        date_before = (dt.datetime.strptime(date_str, '%Y-%m-%d') - dt.timedelta(days=30)).strftime('%Y-%m-%d')
        trade_days = ts.get_k_data("000001", date_before, date_str)["date"].values
        sql = "select date, code, close from daily_k where date <='%s' and date >= '%s'" % (trade_days[-1], trade_days[-size])
        df = pd.read_sql(sql, self.engine)
        # df_tmp = pd.DataFrame({'date':trade_days[-size],'close':[1]*size})
        # df_lst = []
        array = []
        codes = df['code'].drop_duplicates().values
        for code in codes:
            df_new = df[df.code == code]
            if len(df_new) != size:
                avg = df_new['close'].sum()/len(df_new)
                # df_new = pd.merge(df_new, df_tmp)
                array.append(np.array([avg]*size))
            else:
                array.append(df_new['close'].values)
        cov = np.cov(array)

        return pd.DataFrame(cov, index=codes, columns=codes)


# plt.figure()
# plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
# plt.rcParams['axes.unicode_minus']=False #用来正常显示负号

# dir_name = 'lianghua3'
# # dir_name = 'longwu'
# file_lst = os.listdir(dir_name)
# s_h = stock_holding()
# # track_lst = []
# # name_lst = []
# for name in file_lst:
#     if len(name) < 5:
#         continue
#     date = name.split('.')[0]
#     print date
#
#     # stock_df = get_stock_holding(dir_name + "/" + date)
#     # stock_df = get_stock_holding_pandas(dir_name + "/" + date)
#     bench_stock_df = get_index_stocks('000300.SH', date)
#     # stock_dif_df = get_excess_stock(stock_df, bench_stock_df)
#
#     # cap_df = s_h.get_cap_percent(stock_df, bench_stock_df, date)
#     # indu_df = s_h.get_inds_percent(stock_df, bench_stock_df)
#     #
#     # fact_df_porfolio = s_h.get_factor_percent(stock_df, date)
#     # fact_df_benchmark = s_h.get_factor_percent(bench_stock_df, date)
#     # fact_df = pd.concat([fact_df_porfolio, fact_df_benchmark])
#     # fact_df.index = ['p','b']
#     #
#     # cap_df.to_csv(dir_name +'/data/cap/cap_df_'+date+'.csv',encoding='utf-8')
#     # indu_df.to_csv(dir_name +'/data/indu/indu_df_'+date+'.csv',encoding='utf-8')
#     # fact_df.to_csv(dir_name +'/data/fact/fact_df_'+date+'.csv',encoding='utf-8')
#
#     # get_stock_risk(date, stock_df, dir_name +'/data/stock_cov/stock_cov_', dir_name +'/data/stock_risk/stock_risk_')
#     # get_stock_risk(date, stock_dif_df, dir_name + '/data/stock_cov/stock_cov_', dir_name + '/data/stock_excess_risk/stock_excess_risk_')
#     # track_lst.append(get_track_error(stock_df, bench_stock_df, date, dir_name +'/data/stock_cov/stock_cov_'))
#     # name_lst.append(date)
#     # get_fact_risk(date, dir_name +'/data/fact/fact_df_', dir_name +'/data/fact_cov/fact_cov_', dir_name +'/data/fact_risk/fact_risk_')
#
# get_factor_return_in_time('2017-05-01', '2017-06-01')
#
# # get_stock_cov_in_time('2016-11-22', '2017-03-01')
# # get_factor_cov_in_time('2016-11-01', '2017-03-01')
# # cal_df_dif(dir_name +'/data/cap', dir_name +'/data/cap_dif.csv')
# #
# # cal_df_dif(dir_name +'/data/indu', dir_name +'/data/indu_dif.csv')
# # cal_df_dif(dir_name +'/data/fact', dir_name +'/data/fact_dif.csv')
# # track_df = pd.DataFrame(track_lst, index=name_lst)
# # track_df.to_excel(dir_name +'/data/track_error.xlsx')
#
# # get_fact_alpha(dir_name +'/data/return', dir_name + '/data/fact_dif.csv', dir_name + '/data/fact_alpha.xlsx')
# pass
