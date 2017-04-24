#coding:utf-8

import platform, os
from sqlalchemy import create_engine

host_mysql = '218.25.140.183'
# host_mysql = 'localhost'
user_mysql = 'root'
# pwd_mysql = 'tczb'
pwd_mysql = '123456'
db_name_mysql = 'stock'

class dao():
    def __init__(self):
        pass

    def get_engine(self):
        self.engine = create_engine('mysql+mysqldb://%s:%s@%s/%s' % (user_mysql, pwd_mysql, host_mysql, db_name_mysql), connect_args={'charset':'utf8'})
        return self.engine