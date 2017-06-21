# -*- coding: utf-8 -*-
"""
Created on Thu May 25 11:16:29 2017

@author: taicheng
"""
import pandas as pd
from sklearn import linear_model
from numpy import *

reg=linear_model.LinearRegression()

def fama_macbeth_regress(Y,X):
    coef=[]
    fl=X['c'].drop_duplicates().values
    for lb in fl:
        data=X[X['c']==lb]
        data1=Y[Y['c']==lb]
        X_lb=data[data.columns[1:]]
        Y_lb=data1[data1.columns[1:]]
        reg.fit(X_lb,Y_lb)
        coef.append(list(reg.coef_[0]))
    xs=pd.DataFrame(coef,index=fl)
    xss=xs.mean()
    xsd=xs.std()
    t_stats=sqrt(len(fl))*xss/xsd
    return xs,xss,xsd,t_stats
    
Y=pd.DataFrame([[1,12],[1,45],[1,32],[2,46],[2,21],[2,56]],columns=['c','y'])
X=pd.DataFrame([[1,3,5],[1,6,2],[1,7,1],[2,9,3],[2,4,8],[2,5,2]],columns=['c','x1','x2'])
xs,xss,xsd,t_stats=fama_macbeth_regress(Y,X)    
    


