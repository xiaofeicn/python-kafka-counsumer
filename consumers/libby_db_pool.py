# -*- coding:utf-8 -*-
from DBUtils.PooledDB import PooledDB
import pymssql  # sqlserver数据库适配器
import datetime

from pymssql import OperationalError, InternalError, ProgrammingError

HOST = "172.16.90.101"
PORT = "1433"
CHARSET = "utf8"
NAME = "game_1003_log"
USER = "sa"
PASSWORK = "cms"

conn_args = {
    'host': "%s" % HOST,
    'port': "%s" % PORT,
    'database': "%s" % NAME,
    'charset': "%s" % CHARSET,
    'user': "%s" % USER,
    'password': "%s" % PASSWORK
}

"""
    mincached : 启动时开启的闲置连接数量(缺省值 0 以为着开始时不创建连接)
    maxcached : 连接池中允许的闲置的最多连接数量(缺省值 0 代表不闲置连接池大小)
    maxshared : 共享连接数允许的最大数量(缺省值 0 代表所有连接都是专用的)如果达到了最大数量,被请求为共享的连接将会被共享使用
    maxconnecyions : 创建连接池的最大数量(缺省值 0 代表不限制)
    blocking : 设置在连接池达到最大数量时的行为(缺省值 0 或 False 代表返回一个错误<toMany......>; 其他代表阻塞直到连接数减少,连接被分配)
    maxusage : 单个连接的最大允许复用次数(缺省值 0 或 False 代表不限制的复用).当达到最大数时,连接会自动重新连接(关闭和重新打开)
    setsession : 一个可选的SQL命令列表用于准备每个会话，如["set datestyle to german", ...]

"""

args = (10, 10, 30, 100, True, 0, None)


class DbManager():
    def __init__(self):
        try:
            self._pool = PooledDB(pymssql, *args, **conn_args)
        except Exception as e:
            print("The parameters for DBUtils is:", conn_args)

    def _getConn(self):
        return self._pool.connection()


_dbManager = DbManager()


def getConn():
    """ 获取数据库连接 """
    return _dbManager._getConn()


def _reConn():
    """ 重新连接数据库 """
    global _dbManager
    re = False
    try:
        _dbManager = DbManager()
        re = True
    except:
        import traceback
        traceback.print_exc()
    finally:
        return re





def reConn():
    print("%s: now try to reconnect Database!" % (datetime.datatime.now()))
    flag = _reConn()
    if flag:
        print("%s reconnect database success!" % (datetime.datatime.now()))
    else:
        print("%s reconnect database failed!" % (datetime.datatime.now()))
