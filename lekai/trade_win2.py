import pandas as pd
import openpyxl

name = 'bai4.xlsx'
wb = openpyxl.load_workbook(name)
sheets = wb.get_sheet_names()

net_lst = []

df_dict = pd.read_excel(name,sheetname=sheets[1:])
for sheet in df_dict.keys():
    data = df_dict[sheet].ix[len(df_dict[sheet])-3,:].values
    for i in range((len(data)-1)/6):
        # if type(data[4*i+1]) == unicode:
        #     pass
        if data[6*i+2] != 0:
            net_lst.append(data[6*i+2])

net_df = pd.DataFrame(net_lst, columns=['m'])

print net_df[net_df.m >0].mean().values[0], net_df[net_df.m <0].mean().values[0]
print len(net_df[net_df.m >0]), len(net_df[net_df.m <0])
