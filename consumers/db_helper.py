# -*- coding:utf-8 -*-
from consumers.libby_db_pool import getConn, reConn
from consumers.libby_db_pool import OperationalError, InternalError, ProgrammingError
import traceback


def test_conn():
    """
        测试连接池连接是否正常
        return：
        res：True：正常，False：不正常
        msg：如果不正常，为异常信息
    """

    test_sql = """
        select 1

    """

    conn = None
    cur = None
    res = False
    msg = ""
    try:
        conn = getConn()
        cur = conn.cursor()
        cur.execute(test_sql)
        res = cur.fetchall()
        res = True
    except Exception as  e:
        # trackback.print_exc()
        print(e)
        msg = e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        return res, msg


def call_reConn():
    """
        重新创建连接池
    """
    reConn()


def p_query(sql, trackback=None):
    """
        dbutils 数据连接池
            只能执行数据查询sql语句,否则会抛错
        @parm: 要执行的sql语句
        @return:
            []:查询结果为空
            None:sql语句执行失败,出现异常
                    二维list:正常结果
    """

    conn = None
    cur = None
    res = None
    try:
        conn = getConn()
        cur = conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
    except (OperationalError, InternalError):
        call_reConn()
        trackback.print_exc()
    except:
        trackback.exc()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        return res


def p_query_one(sql):
    """
        dbutils 数据连接池
                只能执行数据查询sql语句，否则会报错
                执行sql查询语句，获取第一条记录
        @parm：要执行的sql语句
        @return：
            []:查询结果为空
            None:sql语句执行失败,出现异常
            list:正常结果
    """

    conn = None
    cur = None
    res = None
    try:
        conn = getConn()
        cur = conn.cursor()
        cur.execute(sql)
        res = cur.fetchone()
    except (OperationalError, InternalError):
        call_reConn()
    except:
        traceback.print_exc()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        return res


def p_execute(sql):
    """
        dbutils 数据连接池
                执行数据操作语句，包括 update,insert,delete
        @parm:要执行的sql
        @return:
            None:sql语句执行失败,出现异常
            number:影响记录条数
            -2:数据库连接失败导致执行失败
    """
    conn = None
    cur = None
    res = None
    try:
        conn = getConn()
        cur = conn.cursor()
        cur.execute(sql)
        res = cur._cursor.rowcount
        conn.commit()
    except Exception as  e:
        if conn:
            conn.rollback()
        traceback.print_exc()
    finally:
        if res == -1:  # 可能是数据库断开连接
            ret, msg = test_conn()
            if not ret:
                call_reConn()
                res = -2
        if cur:
            cur.close()
        if conn:
            conn.close()
        return res


def p_mutiexec(sql_list):
    """
        dbutils 数据连接池
                执行多条数据操作语句,可以用于多条sql语句的事务性操作,包括 update,insert,delete

        @parm:要执行的sql语句[]
        @return:
            (flag,res):
                flag<Ture or False>:批次是否全部执行成功
                res<list>:每天sql语句执行影响的行数,如果执行失败,由此可以判断第几条sql语句执行失败
                            如果遇到数据库断开的情况,返回[-2,]

    """

    conn = None
    cur = None
    res = []
    flag = True

    try:
        conn = getConn()
        cur = conn.cursor()
        for sql in sql_list:
            cur.execute(sql)
            num = cur._cursor.rowcount
            res.append(num)
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        traceback.print_exc()
    finally:
        if -1 in res:
            ret, msg = test_conn()
            if not ret:
                call_reConn()
                flag = False
                res = [-2, ]
        if cur:
            cur.close()
        if conn:
            conn.close()
        return flag, res


def p_mutiexec_all(parm,thread_name):
    """
        dbutils 数据连接池
                执行多条数据操作语句,可以用于多条sql语句的事务性操作,包括 update,insert,delete

        @parm:{sql:[(),()]}
        @return:
            (flag,res):
                flag<Ture or False>:批次是否全部执行成功
                res<list>:每天sql语句执行影响的行数,如果执行失败,由此可以判断第几条sql语句执行失败
                            如果遇到数据库断开的情况,返回[-2,]

    """

    conn = None
    cur = None
    flag = True

    try:
        conn = getConn()
        cur = conn.cursor()
        for sql in parm.keys():
            # print("name",name,"sql:", sql, "\tvalue:", parm[sql])
            try:
                cur.executemany(sql, parm[sql])
            except Exception as e:
                # print("----------")
                # print(thread_name,traceback.format_exc() )
                print( parm[sql])
                print( type(parm[sql]))
                msg=';'.join(','.join(v) for v in parm[sql])
                print(msg)
                Exception_db(thread_name,str(traceback.format_exc()).replace("'","\""),msg)
                # Exception_db(thread_name,str(traceback.format_exc()).replace("'","\""),';'.join(parm[sql]).replace("'","\""))
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        flag=False


        # traceback.print_exc()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        return flag

def test():
    sql1 = "insert into kafka_config(topic,partition,offset) VALUES (%s,%s,%s)"
    value1 = [('xiaofei', '0', '1'), ('xiaofei', '1', '1'), ('xiaofei', '1', '1')]
    sql2 = "insert into kafka_config_copy(topic,partition,offset) VALUES (%s,%s,%s)"
    value2 = [('xiaofei', '0', 'sad1'), ('xiaofei', '1', '1'), ('xiaofei', '1', '1')]

    dic = {}
    dic[sql1] = value1
    dic[sql2] = value2
    result=p_mutiexec_all(dic,"t")
    return result

# test()
# res=p_query_one("select topic,partition,offset from kafka_config WHERE partition=1 AND  topic='test'")
def Exception_db(thread_name,traceback,msg):
    print(thread_name)
    print(msg)
    sql="insert into game_1003_log.dbo.exception(project,thread,traceback,msg)VALUES ('%s','%s','%s','%s')" %("python_kafka",thread_name,traceback,msg)
    p_execute(sql)


def test1():
    sql1 = "insert into kafka_config(topic,partition,offset) VALUES (%s,%s,%s)"
    value1 = [('xiaofei', '0', '1'), ('xiaofei', '1', '1'), ('xiaofei', '1', '1')]
    sql2 = "insert into kafka_config_copy(topic,partition,offset) VALUES (%s,%s,%s)"
    value2 = [('xiaofei', '0', 'sad1'), ('xiaofei', '1', '1'), ('xiaofei', '1', '1')]

    dic = {}
    dic[sql1] = value1
    dic[sql2] = value2
    result=p_mutiexec_all(dic,"t")
    return result


# test1()