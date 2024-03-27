#!/usr/bin/python
# -*- coding: utf8 -*-
# 使用的 Python2.7
import MySQLdb

# 打开数据库连接
db = MySQLdb.connect("ip","用户","密码","数据库",port=端口)

# 使用cursor()方法创建一个游标对象cursor
cursor = db.cursor()

# 读取SQL文件
with open('/xxxxx/dep_mysql.sql') as f:
    sql_code = f.read()

# 执行SQL文件中的SQL语句
sql_commands = sql_code.split(';')

for command in sql_commands:
    try:
        print('即将执行SQL: \n')
        print(command)
        cursor.execute(command)
    except Exception as e:
        print(e)

# 提交事务
db.commit()

# 关闭数据库连接
db.close()
