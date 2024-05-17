#!/usr/bin/python
# -*- coding: utf8 -*-
## 定时清理调度工作流记录,入参是日期

import io
import subprocess
import requests
import json
import time
import datetime

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(module)s : %(message)s', level=logging.INFO,
                    stream=sys.stdout)
logger = logging.getLogger(__name__)

# 配置信息: ip 端口 token自行修改
base_url = 'http://IP:端口'
token = 'xxxxxxxxxxxxx'

# 获取项目列表
def get_project_list():
    url = "{base_url}/dolphinscheduler/projects?pageSize=100&pageNo=1&searchVal=&_t=0.3741042528841678".format(base_url=base_url)
    payload={}
    headers = {
      'Connection': 'keep-alive',
      'Accept': 'application/json, text/plain, */*',
      'language': 'zh_CN',
      'sessionId': '680b2a0e-624c-4804-9e9e-58c7d4a0b44c',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
      'Referer': "{base_url}/dolphinscheduler/ui/".format(base_url=base_url),
      'Accept-Language': 'zh-CN,zh;q=0.9,pt;q=0.8,en;q=0.7',
      'token':token
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    totalList = response_data['data']['totalList']
    return totalList

def get_page_detail(code,dt):
    url = "{base_url}/dolphinscheduler/projects/{code}/process-instances?searchVal=&pageSize=50&pageNo=1&host=&stateType=&startDate=2000-01-01 00:00:00&endDate={dt} 23:59:59&executorName=".format(code=code,dt=dt,base_url=base_url)
    payload={}
    headers = {
      'Connection': 'keep-alive',
      'Accept': 'application/json, text/plain, */*',
      'language': 'zh_CN',
      'sessionId': '680b2a0e-624c-4804-9e9e-58c7d4a0b44c',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
      'Referer': "{base_url}/dolphinscheduler/ui/".format(base_url=base_url),
      'Accept-Language': 'zh-CN,zh;q=0.9,pt;q=0.8,en;q=0.7',
      'token':token
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    page = response_data['data']['totalList']
    page_del = 'processInstanceIds='
    if len(page) == 0:
        print('列表为空,退出程序')
        return '0'
    for p in page:
        page_del = page_del + str(p['id']) + ','
    # print(page_del)
    return page_del

def delete(project,ids):
    print('即将删除如下工作流实例:')
    print(project)
    print(ids)
    url = "{base_url}/dolphinscheduler/projects/{project}/process-instances/batch-delete".format(base_url=base_url,project = project)
    # 'processInstanceIds=89767'
    payload= ids
    headers = {
      'Connection': 'keep-alive',
      'Accept': 'application/json, text/plain, */*',
      'language': 'zh_CN',
      'sessionId': '680b2a0e-624c-4804-9e9e-58c7d4a0b44c',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
      'Content-Type': 'application/x-www-form-urlencoded',
      'Referer': "{base_url}/dolphinscheduler/ui/".format(base_url=base_url),
      'Accept-Language': 'zh-CN,zh;q=0.9,pt;q=0.8,en;q=0.7',
      'token':token
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    print('执行结果如下:')
    print(response.text)

if __name__ == '__main__':
    #获取请求参数()
    params = {"dt": "dt"};
    parser = get_option_parser(params)
    options, args = parser.parse_args(sys.argv[1:])
    logger.info('开始执行删除任务实例...' + " ".join(sys.argv))
    # 清理的日期
    dt = options.dt
    if dt == '' or len(dt) == 0:
        logger.error('调度系统-运维任务:日期为空，请输入日期')
        sys.exit(1)

    today_91 = (datetime.datetime.now()+datetime.timedelta(days=-61)).strftime("%Y-%m-%d")

    short_dt = dt.replace('-','')
    short_today_91 = today_91.replace('-','')
    if int(short_dt) > int(short_today_91):
        logger.error('调度系统-运维任务:不能删除最近90天之内的任务实例')
        sys.exit(1)
    # # 需要处理的项目
    projects = get_project_list()
    # 依次处理项目
    for project in projects:
        code = project['code']
        print('正在处理:'+ str(code))
        while True:
            page_del = get_page_detail(code,dt)
            if page_del == '0':
                break
            delete(code,page_del)
            time.sleep(1)
