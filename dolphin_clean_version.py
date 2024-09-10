#!/usr/bin/python
# -*- coding: utf8 -*-
## 清理调度任务历史版本记录，依然是使用API的方式，直接操作数据库风险比较高。
## 会减少 process_definition_log 和 process_task_relation_log 的数据。

import io
import subprocess
import requests
import json
import time
import datetime

# 配置信息: ip 端口 token自行修改
base_url = 'http://xxxx:xxxx'
token = 'xxxxx'
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

# 获取工作定义列表
def get_definition_detail(project_code):
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
    all_data = []
    pageNo = 1
    while True:
        url = "{base_url}/dolphinscheduler/projects/{project_code}/process-definition?searchVal=&pageSize=50&pageNo={pageNo}".format(project_code=project_code,pageNo=pageNo,base_url=base_url)
        response = requests.request("GET", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        page_data = response_data['data']['totalList']
        totalPage = response_data['data']['totalPage']

        if len(page_data) == 0:
            print('工作定义列表为空,退出循环...')
            break
        all_data.extend(page_data)

        if pageNo >= totalPage:
            print('工作定义列表到头了,退出循环...')
            break
        pageNo += 1
    # 返回全部数据
    return all_data

# 获取工作定义的版本信息列表,注意，这里从第二页开始！！！size是 20
def get_version_detail(project_code,dag_code,current_version):
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

    all_version = []
    pageNo = 2

    while True:
        if pageNo <= 1:
            print('获取工作定义的版本信息列表,pageNo 必须大于1！！！')
            break

        url = "{base_url}/dolphinscheduler/projects/{project_code}/process-definition/{dag_code}/versions?searchVal=&pageSize=20&pageNo={pageNo}".format(project_code=project_code,dag_code=dag_code,pageNo=pageNo,base_url=base_url)
        response = requests.request("GET", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        page_data = response_data['data']['totalList']
        totalPage = response_data['data']['totalPage']

        if len(page_data) == 0:
            print('version列表为空,退出循环...')
            break

        for page in page_data:
            version = int(page['version'])
            # 保留近20个版本
            if version + 20 <= current_version:
                all_version.append(version)

        if pageNo >= totalPage:
            print('version列表到头了,退出循环...')
            break

        pageNo += 1

    # TODO 分析all_data里面是否包含 current_version

    # 返回正常的数据
    return all_version

def delete(project_code,dag_code,version):
    print('即将删除的项目，工作流以及版本')
    print(project_code)
    print(dag_code)
    print(version)
    url = "{base_url}/dolphinscheduler/projects/{project_code}/process-definition/{dag_code}/versions/{version}".format(project_code=project_code,dag_code=dag_code,version=version,base_url=base_url)
    # 'processInstanceIds=89767'
    payload={}
    headers = {
      'Connection': 'keep-alive',
      'Accept': 'application/json, text/plain, */*',
      'language': 'zh_CN',
      'sessionId': '680b2a0e-624c-4804-9e9e-58c7d4a0b44c',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept-Language': 'zh-CN,zh;q=0.9,pt;q=0.8,en;q=0.7',
      'token':token,
      'Cookie': 'sessionId=680b2a0e-624c-4804-9e9e-58c7d4a0b44c; language=zh_CN; userName=admin; HERA_Token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzc29JZCI6Ii0xIiwic3NvX25hbWUiOiJhZG1pbiIsImF1ZCI6IjJkZmlyZSIsImlzcyI6ImhlcmEiLCJleHAiOjE2NDYwMjk3MDYsInVzZXJJZCI6IjEiLCJpYXQiOjE2NDU3NzA1MDYsInVzZXJuYW1lIjoiYWRtaW4ifQ.YEhr9Mi7FDsQIAn5GJorB0U3lL92KQA8YvP26QMhh9g; sessionId=680b2a0e-624c-4804-9e9e-58c7d4a0b44c'
    }
    response = requests.request("DELETE", url, headers=headers, data=payload)
    print('执行结果如下:')
    print(response.text)

if __name__ == '__main__':
    # # 需要处理的项目
    projects = get_project_list()
    # 依次处理项目
    for project in projects:
        project_code = project['code']
        print('正在处理项目:'+ str(project_code))
        all_dags = get_definition_detail(project_code)
        for dag in all_dags:
            # 工作流code和当前版本
            dag_code = dag['code']
            current_version = dag['version']
            print(dag_code)
            print(current_version)
            # 获取该工作流历史版本记录...
            all_data = get_version_detail(project_code,dag_code,current_version)
            # TODO 删除
            print(all_data)
            for v in all_data:
                delete(project_code,dag_code,v)
