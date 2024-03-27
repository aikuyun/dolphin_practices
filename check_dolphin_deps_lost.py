#!/usr/bin/python
# -*- coding: utf8 -*-
## 检测依赖丢失

import io
import subprocess
import requests
import json
import time
import datetime
from common import *

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(module)s : %(message)s', level=logging.INFO,
                    stream=sys.stdout)
logger = logging.getLogger(__name__)

def sendGoodNewsDingDing(text):
    webhook="https://oapi.dingtalk.com/robot/send?access_token=xxxxxxxxxxxxx"
    # print(text)
    headers = {'Content-Type': 'application/json'}
    webhook = webhook
    data = {
      "markdown": {
            "title":"调度系统依赖缺失,生产问题!",
              "text": text
           },
        "msgtype": "markdown",
        # "text": {
        #     "content":
        # },
        "at": {
            "atMobiles": [],
            "isAtAll": False
        }
    }
    x = requests.post(url=webhook, data=json.dumps(data), headers=headers)

def get_dep_los_sql(conn):
    cursor = conn.cursor()
    select_sql = """
    select
     node.project_name as pre_project_name
    ,t.pre_project_code as pre_project_code
    ,node.dag_name as pre_dag_name
    ,t.pre_dag_code as pre_dag_code
    ,t.pre_task_code as pre_task_code
    ,t.post_project_name as post_project_name
    ,t.post_project_code as post_project_code
    ,t.post_dag_name as post_dag_name
    ,t.post_dag_code as post_dag_code
    ,t.post_dag_version as post_dag_version
    ,t.post_task_code as post_task_code
    ,t.post_task_name as post_task_name
    ,t.post_task_type as post_task_type
    from (
      select
       node.project_name as pre_project_name
      ,t2.pre_project_code as pre_project_code
      ,node.dag_name as pre_dag_name
      ,t2.pre_dag_code as pre_dag_code
      ,t2.pre_task_code as pre_task_code
      ,t2.post_project_name as post_project_name
      ,t2.post_project_code as post_project_code
      ,t2.post_dag_name as post_dag_name
      ,t2.post_dag_code as post_dag_code
      ,t2.post_dag_version as post_dag_version
      ,t2.post_task_code as post_task_code
      ,t2.post_task_name as post_task_name
      ,t2.post_task_type as post_task_type
      from (select * from t_ds_dag_task_relation_dep_data_df where pre_task_code != 0 ) as t2
      left JOIN t_ds_task_node_base_data as node
      on t2.pre_project_code = node.project_code
      and t2.pre_dag_code = node.dag_code
      and t2.pre_task_code = node.task_code
      where node.project_name is  null
      group by
       node.project_name
      ,t2.pre_project_code
      ,node.dag_name
      ,t2.pre_dag_code
      ,t2.pre_task_code
      ,t2.post_project_name
      ,t2.post_project_code
      ,t2.post_dag_name
      ,t2.post_dag_code
      ,t2.post_dag_version
      ,t2.post_task_code
      ,t2.post_task_name
      ,t2.post_task_type
    ) as t
    left join (select project_name,project_code,dag_name,dag_code from t_ds_task_node_base_data group  by  project_name,project_code,dag_name,dag_code) as node
    on t.pre_project_code = node.project_code
    and t.pre_dag_code = node.dag_code
    """
    cursor.execute(select_sql)
    results = cursor.fetchall()
    return results

# 程序入口
if __name__ == '__main__':
    logger.info('开始巡检调度依赖....')
    try:
        conn = get_dolphi_db_connection()
    except Exception as e:
        logger.error('获取调度系统的Mysql连接失败,错误信息：%s' % e)
        sendDingDing('获取调度系统的Mysql连接失败,错误信息：%s' % e)
        sys.exit(1)

    values = get_dep_los_sql(conn)

    if len(values) == 0:
        logger.info('【调度执行】没有丢失的依赖....')
    else:
        # 循环执行
        lines = '[惊愕] [惊愕] [惊愕] \n\n ## <font color=#FF0000 face="黑体">下面这些任务缺失依赖！！！</font>'
        for v in values:
            pre_project_name = v[0]
            pre_project_code = v[1]
            pre_dag_name = v[2]
            pre_dag_code = v[3]
            pre_task_code = v[4]
            post_project_name = v[5]
            post_dag_name = v[7]
            post_task_name = v[11]
            lines = lines + '\n\n 项目名: {post_project_name} \n\n 工作流定义: {post_dag_name} \n\n  任务名: {post_task_name} \n\n 任务类型: 依赖节点  \n\n <font color=#FF0000 face="黑体"> 依赖项目: {pre_project_name} </font> \n\n  <font color=#FF0000 face="黑体"> 依赖工作流名称: {pre_dag_name} </font> \n\n  <font color=#FF0000 face="黑体"> 依赖任务code: {pre_task_code} </font> \n\n   -------------------------- '.format(post_project_name=post_project_name,post_dag_name=post_dag_name,post_task_name=post_task_name,pre_project_name=pre_project_name,pre_dag_name=pre_dag_name,pre_task_code=pre_task_code)
        lines = lines + '\n \n 依赖缺失的可能原因: \n\n - 上游任务被下线 \n\n - 上游任务被禁用 \n\n - 上游任务发生迁移 \n\n'
        sendGoodNewsDingDing(lines)
