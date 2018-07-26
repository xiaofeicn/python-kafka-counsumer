# -*- coding:utf-8 -*-
from consumers.db_helper import *


def get_kafka(topic, partition):
    sql = "select topic,partition,offset from kafka_config WHERE  partition=%s AND  topic='%s'" % (partition, topic)
    result = p_query_one(sql)
    dic = {}
    dic['topic'] = result[0]
    dic['partition'] = result[1]
    dic['offset'] = result[2]
    return dic


def save_to_db(parm,thread_name):
    return  p_mutiexec_all(parm,thread_name)




def update_offset(topic, partition, offset):
    sql = "update kafka_config set offset=%s WHERE  topic='%s' and partition=%s" % (offset, topic, partition)
    result = p_execute(sql)
    return result

def exp(msg):
    sql = "insert into game_1003_log.dbo.thread_die(msg) VALUES('%s') " % (msg)
    result = p_execute(sql)
    return result




def get():
    result = p_query("select name,Expr1 from v_log_table ")
    col_dic = {}
    sql_dic = {}

    for re in result:
        col_dic[re[0]] = re[1]
        sql = "insert into game_1003_log.dbo.%s (%s) VALUES " % (re[0], re[1])
        value = ""
        for x in range(0, len(str(re[1]).split(","))):
            value += ",%s"
        value = "(" + value.lstrip(",") + ")"
        sql += value
        sql_dic[re[0]] = sql
    return col_dic, sql_dic


