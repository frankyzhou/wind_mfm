# coding=utf-8
import xlrd
from init import *
import pandas as pd
import matplotlib.pyplot as plt

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
    for rownum in range(5,nrows):
         row = table.row_values(rownum)
         if row:
             app = {}
             for i in range(len(colnames)):
                app[colnames[i]] = row[i]
             list.append(app)
    # for i in range(nrows):
    #     list.append(table.row_values(i))
    #     for j in table.row_values(i):
    #         print j
    #     print "-"*50
    return list

def get_stock_holding():
    stock_lst = []
    code = u'科目代码'
    percent = u'市值占净值%'
    tables = excel_table_byindex('holdings.xls', 3)
    for row in tables:
        if row[code][:4] == '1102' and len(row[code]) > 10:
            stock = {}
            stock_code = row[code][-6:]
            maker = "H" if stock_code[0] == '6' else "Z"
            stock["code"] = stock_code + ".S" + maker
            stock["percent"] = row[percent]
            stock_lst.append(stock)

    return stock_lst

class stock_holding():
    def __init__(self):
        self.db = dao()
        self.engine = self.db.get_engine()

    def get_cap_percent(self, stock_lst):
        sql = "select code, mktcap from daily_factors where date='%s'" % '2017-04-19'
        df = pd.read_sql(sql, self.engine)
        cap_pct = {}
        cap_pct["l"] = 0
        cap_pct["m"] = 0
        cap_pct["s"] = 0
        for s in stock_lst:
            cap = df[df.code == s["code"]]["mktcap"].values[0] / 100000000
            if cap < 100:
                cap_pct["s"] += s["percent"]
            elif cap < 500 and cap > 100:
                cap_pct["m"] += s["percent"]
            else:
                cap_pct["l"] += s["percent"]
        df_cap = pd.DataFrame(cap_pct.items())
        df_cap.index = df_cap.ix[:, 0].values
        # plt.figure()
        df_cap.plot(kind="bar")
        plt.savefig("cap_pct.png")
        return cap_pct

    def get_inds_percent(self, stock_lst):
        sql = "select code, industry from stock_info"
        df = pd.read_sql(sql, self.engine)
        industry_set = set()
        industry_pct = {}
        for i in df["industry"].values:
            industry_set.add(i)

        for s in stock_lst:
            industry = df[df.code == s["code"]]["industry"].values[0]
            if industry not in industry_pct.keys():
                industry_pct[industry] = 0
            else:
                industry_pct[industry] += s["percent"]
        df_inst = pd.DataFrame(industry_pct.items())
        df_inst.index = df_inst.ix[:,0].values
        # plt.figure()
        df_inst.plot(kind="bar")
        plt.savefig("indu_pct.png")
        return industry_pct


plt.figure()
plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号

stock_lst = get_stock_holding()
s_h = stock_holding()
s_h.get_cap_percent(stock_lst)
s_h.get_inds_percent(stock_lst)