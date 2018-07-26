#!/usr/bin/python3
# -*- coding: utf-8 -*-

from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr

import smtplib

def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(),addr))
'''
from_addr = input('From: ')
password = input('Password: ')
to_addr = input('To: ')
smtp_server = input('SMTP server: ')

'''
import smtplib
from_addr ="SendUserEmail"
password = "SendUserPwd"
to_addr = "xiaofei@baiwen100.com"
smtp_server = 'smtp.exmail.qq.com'
def send_msg(msg):
    msg = MIMEText(msg, 'plain', 'utf-8')
    msg['From'] = _format_addr('python-kafka <%s>' % (from_addr))
    msg['To'] = _format_addr('管理员 <%s>' % to_addr)
    msg['Subject'] = Header('python-kafka', 'utf-8').encode()
    server = smtplib.SMTP(smtp_server, 25)
    server.set_debuglevel(1)
    server.login(from_addr, password)
    server.sendmail(from_addr, [to_addr], msg.as_string())
    server.quit()

# send_msg("test")