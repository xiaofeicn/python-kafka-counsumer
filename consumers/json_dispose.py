# coding=utf-8
import json
import time


def get_line(col_dic, data):
    # data='{"f_time":1515656300,"f_server_lan_address":"10.104.255.101","f_server_wan_address":"","f_params":{"f_dept":"3","f_character_grade":"8","f_server_address_id":"14","f_character_ip":"1.197.235.240","f_time":"2018-01-11 15:38:20","f_channel":"1002","f_game_id":"1","f_sid":"14","f_yunying_id":"7EB01FECA1AE84D42A8124D3497F878E","f_character_id":"14001067"},"f_game_id":1,"f_log_name":"log_uplevel"}'
    # string=data.replace("\"f_params\":{","").replace("},",",")
    data_dic = get_dic(data)
    if data_dic is None:
        return None, None
    log_name = None
    try:
        log_name = data_dic['f_log_name']
    except Exception as e:
        print(e)
    finally:
        if log_name is None:
            print("%s 日志对应表不存在")
            return None, None
        cols = col_dic[log_name]
        cols_list = cols.split(",")
        values = []
        for col in cols_list:
            # 当col不存在json中，value默认为“-1”
            col_data = data_dic.get(col, "-1")
            if col == "f_time" and str(col_data).isdigit() is True:
                # 时间戳转换格式化时间
                col_data = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(col_data)))
            values.append(col_data)
        return values, log_name


def get_dic(data):
    string = data.replace("\"f_params\":{", "").replace("},", ",").lower()
    data_dic = None
    # print("get_dic")
    try:
        data_dic = json.loads(string)
    except:
        print("json erro")

    return data_dic
