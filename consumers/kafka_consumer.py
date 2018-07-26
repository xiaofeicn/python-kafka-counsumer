import threading

import os
import sys
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

from consumers.db_util import *
from consumers.json_dispose import *
from collections import OrderedDict
from consumers.mail import send_msg
import time

threads = []
threads_name = ["MainThread", "Thread-1", "Thread-2", "Thread-0"]
col_dic, sql_dic = get()
thread_msg = set()
known = set()

is_dispose = True
'''
如只在错误时不提交偏移量，下次运行错误消息重新处理，可以不用提交到数据库，运行时可以不指定offset
只分配分区就可以，此程序在手动提交offset的同时记录偏移量可以更明确看到偏移量，运行程序时的重置的
偏移量【数据库获得】跟kafka自己记录的消费组最大偏移量是相同的

'''


class MyThread(threading.Thread):
    def __init__(self, thread_name, topic, partition):
        threading.Thread.__init__(self)
        self.thread_name = thread_name
        self.name = thread_name
        # self.keyName = keyName
        self.partition = partition
        self.topic = topic

    def run(self):
        print("Starting " + self.name)
        Consumer(self.thread_name, self.topic, self.partition)

    def stop(self):
        sys.exit()


def Consumer(thread_name, topic, partition):
    print(thread_name, "Starting\tDispose", )
    global is_dispose
    broker_list = '172.16.90.63:6667, 172.16.90.58:6667, 172.16.90.59:6667'
    '''
    fetch_min_bytes（int） - 服务器为获取请求而返回的最小数据量，否则请等待
    fetch_max_wait_ms（int） - 如果没有足够的数据立即满足fetch_min_bytes给出的要求，服务器在回应提取请求之前将阻塞的最大时间量（以毫秒为单位）
    fetch_max_bytes（int） - 服务器应为获取请求返回的最大数据量。这不是绝对最大值，如果获取的第一个非空分区中的第一条消息大于此值，
                            则仍将返回消息以确保消费者可以取得进展。注意：使用者并行执行对多个代理的提取，因此内存使用将取决于包含该主题分区的代理的数量。
                            支持的Kafka版本> = 0.10.1.0。默认值：52428800（50 MB）。
    enable_auto_commit（bool） - 如果为True，则消费者的偏移量将在后台定期提交。默认值：True。
    max_poll_records（int） - 单次调用中返回的最大记录数poll()。默认值：500
    max_poll_interval_ms（int） - poll()使用使用者组管理时的调用之间的最大延迟 。这为消费者在获取更多记录之前可以闲置的时间量设置了上限。
                                如果 poll()在此超时到期之前未调用，则认为使用者失败，并且该组将重新平衡以便将分区重新分配给另一个成员。默认300000
    '''
    consumer = KafkaConsumer(bootstrap_servers=broker_list,
                             group_id="xiaofei",
                             client_id=thread_name,
                             # auto_offset_reset="smallest",
                             enable_auto_commit=False,
                             fetch_min_bytes=1024 * 1024,
                             # fetch_max_bytes=1024 * 1024 * 1024 * 10,
                             fetch_max_wait_ms=60000,
                             request_timeout_ms=305000,
                             # consumer_timeout_ms=1,
                             # max_poll_records=5000,
                             # max_poll_interval_ms=60000 无该参数
                             )
    dic = get_kafka(topic, partition)

    tp = TopicPartition(topic, partition)
    # print(thread_name, tp, dic['offset'])
    consumer.assign([tp])
    # 重定向分区offset
    consumer.seek(tp, dic['offset'])
    print("程序首次运行\t线程:", thread_name, "分区:", partition, "偏移量:", dic['offset'], "\t开始消费...")
    num = 0
    # end_offset = consumer.end_offsets([tp])[tp]
    # print(end_offset)
    while True:
        args = OrderedDict()
        checkThread()
        msg = consumer.poll(timeout_ms=60000)
        end_offset = consumer.end_offsets([tp])[tp]
        print('已保存的偏移量', consumer.committed(tp), '最新偏移量，', end_offset)
        # 测试线程死掉
        # if thread_name=="Thread-1" and num==2:
        #     sys.exit()
        if len(thread_msg) > 0 and is_dispose is True:
            is_dispose = False
            for msg_send in thread_msg:
                exp(msg_send)
                send_msg(msg_send)
            thread_msg.clear()
        if len(msg) > 0:
            print("线程:", thread_name, "分区:", partition, "最大偏移量:", end_offset, "有无数据,", len(msg))
            lines = 0
            for data in msg.values():
                for line in data:
                    lines += 1
                    line = eval(line.value.decode('utf-8'))
                    value, log_name = get_line(col_dic, line)
                    sql = sql_dic[log_name]
                    if value is not None:
                        args.setdefault(sql, []).append(tuple(value))
            print(thread_name, "处理条数", lines)
            # 数据保存至数据库
            is_succeed = save_to_db(args, thread_name)
            if is_succeed:
                # 更新保存在数据库中的分区的偏移量
                is_succeed1 = update_offset(topic, partition, end_offset)
                # 手动提交偏移量到kafka
                consumer.commit(offsets={tp: (OffsetAndMetadata(end_offset, None))})
                # print(thread_name,"to db suss",num+1)
                if is_succeed1 == 0:
                    sys.exit()
            else:
                sys.exit()
        else:
            pass
            # print(thread_name,'没有数据')
        # time.sleep(60)
        num += 1
        # print(thread_name,"第",num,"次")


def checkThread():
    initThreadsName = threads_name
    nowThreadsName = []  # 用来保存当前线程名称
    now = threading.enumerate()  # 获取当前线程名
    for i in now:
        nowThreadsName.append(i.getName())  # 保存当前线程名称

    for thread in initThreadsName:
        if thread in nowThreadsName:
            pass  # 当前某线程名包含在初始化线程组中，可以认为线程仍在运行
            # print(thread, "alive")
        else:
            '''
                     print( '==='+thread,'stopped，now restart')
            t = MyThread("Thread-2", "test", 2)
            t.start()

            '''
            if thread not in known:
                msg = "%s \tis not alive" % (thread)
                thread_msg.add(msg)
                known.add(thread)
                print(thread, "not alive")


if __name__ == '__main__':
    try:
        t1 = MyThread("Thread-0", "test", 0)
        threads.append(t1)
        t2 = MyThread("Thread-1", "test", 1)
        threads.append(t2)
        t3 = MyThread("Thread-2", "test", 2)
        threads.append(t3)

        for t in threads:
            t.start()
            threads_name.append(t.getName())

        for t in threads:
            t.join()

        print("exit program with 0")
    except:
        print("Error: failed to run consumer program")
