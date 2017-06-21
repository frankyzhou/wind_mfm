# coding=utf-8
import numpy as np
import matplotlib.pyplot as plt

#=======自己设置开始============
#标签
labels = np.array([u'估值',u'贝塔',u'成长',u'盈利',u'市值',u'动量'])
#数据个数
dataLenth = 6
#数据
data = np.array([0.58,0.60,0.28,0.67,0.58,0.8])
#========自己设置结束============

angles = np.linspace(0, 2*np.pi, dataLenth, endpoint=False)
data = np.concatenate((data, [data[0]])) # 闭合
angles = np.concatenate((angles, [angles[0]])) # 闭合

fig = plt.figure()
plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号


ax = fig.add_subplot(111, polar=True)# polar参数！！
ax.plot(angles, data, 'bo-', linewidth=2)# 画线
ax.fill(angles, data, facecolor='r', alpha=0.25)# 填充
ax.set_thetagrids(angles * 180/np.pi, labels, fontproperties="SimHei")
ax.set_title(u"风格因子分析", va='bottom', fontproperties="SimHei")
ax.set_rlim(0,1)
ax.grid(True)
plt.show()
