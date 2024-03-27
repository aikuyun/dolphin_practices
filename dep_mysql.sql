DROP table if EXISTS t_ds_dag_task_relation_base_data;
CREATE TABLE `t_ds_dag_task_relation_base_data` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `project_name` varchar(200)  COMMENT '项目名称',
  `project_code` bigint(20)  COMMENT '项目Code',
  `dag_name` varchar(256)  COMMENT '工作流名称',
  `dag_code` bigint(20)  COMMENT '工作流code',
  `dag_version` int(11)  COMMENT '工作流版本',
  `pre_task_code` bigint(20)   COMMENT '上游任务code',
  `pre_task_version` int(11)   COMMENT '上游任务版本',
  `post_task_code` bigint(20)   COMMENT '下游任务code',
  `post_task_version` int(11)   COMMENT '下游任务版本',
  `pre_name` varchar(200) COMMENT '上游任务名称',
  `post_name` varchar(200) COMMENT '下游任务名称',
  `pre_task_type` varchar(50) COMMENT '上游任务类型',
  `pre_task_params` longtext  COMMENT '上游任务信息',
  `post_task_type` varchar(50) COMMENT '下游任务类型',
  `post_task_params` longtext  COMMENT '下游任务信息',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='调度系统工作流任务关系基础数据'
;

-- 查询工作流里面的任务关系：项目名字，DAG名字、任务名字、版本号
insert into t_ds_dag_task_relation_base_data(project_name ,project_code ,dag_name ,dag_code ,dag_version ,pre_task_code ,pre_task_version ,post_task_code ,post_task_version ,pre_name,post_name,pre_task_type ,pre_task_params ,post_task_type ,post_task_params )
SELECT
t_ds_project.name as project_name,t_ds_project.code as project_code , t_ds_process_definition.name as dag_name,t_ds_process_definition.code as dag_code,t_ds_process_definition.version as dag_version
,pre_task_code, pre_task_version,
post_task_code, post_task_version,task1.name as pre_name,task2.name as post_name,task1.task_type as pre_task_type,task1.task_params as pre_task_params,task2.task_type as post_task_type,task2.task_params as post_task_params
FROM t_ds_process_definition
join t_ds_project
on t_ds_project.code = t_ds_process_definition.project_code
join t_ds_process_task_relation_log
on t_ds_process_definition.version = t_ds_process_task_relation_log.process_definition_version
and t_ds_process_definition.code = t_ds_process_task_relation_log.process_definition_code
left join (select * from t_ds_task_definition_log where flag =1)  as task1
on task1.code = t_ds_process_task_relation_log.pre_task_code
and task1.version = t_ds_process_task_relation_log.pre_task_version
left join (select * from t_ds_task_definition_log where flag =1)  as task2
on task2.code = t_ds_process_task_relation_log.post_task_code
and task2.version = t_ds_process_task_relation_log.post_task_version
where t_ds_process_definition.release_state = 1
;


DROP table if EXISTS t_ds_task_node_base_data;
CREATE TABLE `t_ds_task_node_base_data` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `project_name` varchar(200) NOT NULL COMMENT '项目名称',
  `project_code` bigint(20) NOT NULL COMMENT '项目Code',
  `dag_name` varchar(256) DEFAULT NULL COMMENT '工作流名称',
  `dag_code` bigint(20) DEFAULT NULL COMMENT '工作流code',
  `dag_version` int(11) NOT NULL COMMENT '工作流版本',
  `task_code` bigint(20) NOT NULL  COMMENT '任务code',
  `task_version` int(11) NOT NULL  COMMENT '任务版本',
  `task_name` varchar(200) NOT NULL  COMMENT '任务名称',
  `task_type` varchar(50) NOT NULL  COMMENT '任务类型',
  `task_params` longtext  COMMENT '任务信息',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='调度系统工作流任务基础数据'
;
CREATE INDEX idx_project_dag_task ON t_ds_task_node_base_data(project_code, dag_code, task_code);
CREATE INDEX idx_task_type ON t_ds_task_node_base_data(task_type);

insert into t_ds_task_node_base_data(project_name ,project_code ,dag_name ,dag_code,dag_version ,task_code ,task_version ,task_name ,task_type ,task_params  )
SELECT
project_name,
       project_code,
       dag_name,
       dag_code,
       dag_version,
       task_code,
       task_version,
       task_name,
       task_type,
       task_params
FROM(
    SELECT
           project_name,
           project_code,
           dag_name,
           dag_code,
           dag_version,
           pre_task_code as task_code,
           pre_task_version as task_version ,
           pre_name as task_name ,
           pre_task_type as task_type ,
           pre_task_params as task_params
    FROM t_ds_dag_task_relation_base_data
    where pre_task_type is not null
    UNION ALL
    SELECT
           project_name,
           project_code,
           dag_name,
           dag_code,
           dag_version,
           post_task_code as task_code,
           post_task_version as task_version ,
           post_name as task_name ,
           post_task_type as task_type ,
           post_task_params as task_params
    FROM t_ds_dag_task_relation_base_data
    where post_task_type is not null
) as t
group by  project_name,
       project_code,
       dag_name,
       dag_code,
       dag_version,
       task_code,
       task_version,
       task_name,
       task_type,
       task_params
;

DROP table if EXISTS t_ds_dag_task_relation_data_df;
CREATE TABLE `t_ds_dag_task_relation_data_df` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `pre_project_name` varchar(200) NOT NULL COMMENT '项目名称',
  `pre_project_code` bigint(20) NOT NULL COMMENT '项目Code',
  `pre_dag_name` varchar(256) DEFAULT NULL COMMENT '工作流名称',
  `pre_dag_code` bigint(20) DEFAULT NULL COMMENT '工作流code',
  `pre_dag_version` int(11) NOT NULL COMMENT '工作流版本',
  `pre_task_code` bigint(20) NOT NULL  COMMENT '上游任务code',
  `pre_task_name` varchar(200) NOT NULL  COMMENT '上游任务名称',
  `pre_task_type` varchar(50) NOT NULL  COMMENT '上游任务类型',
  `pre_task_version` int(11) NOT NULL  COMMENT '上游任务版本',
  `post_project_name` varchar(200) NOT NULL COMMENT '下游项目名称',
  `post_project_code` bigint(20) NOT NULL COMMENT '下游项目Code',
  `post_dag_name` varchar(256) DEFAULT NULL COMMENT '下游工作流名称',
  `post_dag_code` bigint(20) DEFAULT NULL COMMENT '下游工作流code',
  `post_dag_version` int(11) NOT NULL COMMENT '下游工作流版本',
  `post_task_code` bigint(20) NOT NULL  COMMENT '下游任务code',
  `post_task_name` varchar(200) NOT NULL  COMMENT '下游任务名称',
  `post_task_type` varchar(50) NOT NULL  COMMENT '下游任务类型',
  `post_task_version` int(11) NOT NULL  COMMENT '下游任务版本',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='调度系统工作流任务关系全量数据'
;

-- 第一部分数据
insert into t_ds_dag_task_relation_data_df(pre_project_name,pre_project_code,pre_dag_name,pre_dag_code,pre_dag_version,pre_task_code,pre_task_name,pre_task_type,pre_task_version,post_project_name,post_project_code,post_dag_name,post_dag_code,post_dag_version,post_task_code,post_task_name,post_task_type,post_task_version)
SELECT
project_name as pre_project_name,
project_code as pre_project_code,
dag_name as pre_dag_name,
dag_code as pre_dag_code,
dag_version as pre_dag_version,
pre_task_code as pre_task_code,
pre_name as pre_task_name,
pre_task_type as pre_task_type,
pre_task_version as pre_task_version ,
project_name as post_project_name,
project_code as post_project_code,
dag_name as post_dag_name,
dag_code as post_dag_code,
dag_version as post_dag_version,
post_task_code as post_task_code,
post_name as post_task_name,
post_task_type as post_task_type,
post_task_version as post_task_version
FROM t_ds_dag_task_relation_base_data
where pre_task_type is not null
and post_task_type  is not null
;

-- 用于存放dependent节点的关系数据
DROP table if EXISTS t_ds_dag_task_relation_dep_data_df;
CREATE TABLE `t_ds_dag_task_relation_dep_data_df` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `pre_project_code` bigint(20)  COMMENT '项目Code',
  `pre_dag_code` bigint(20)  COMMENT '工作流code',
  `pre_task_code` bigint(20)  COMMENT '上游任务code',
  `post_project_name` varchar(200)  COMMENT '下游项目名称',
  `post_project_code` bigint(20)  COMMENT '下游项目Code',
  `post_dag_name` varchar(256)  COMMENT '下游工作流名称',
  `post_dag_code` bigint(20)  COMMENT '下游工作流code',
  `post_dag_version` int(11)  COMMENT '下游工作流版本',
  `post_task_code` bigint(20)   COMMENT '下游任务code',
  `post_task_name` varchar(200)  COMMENT '下游任务名称',
  `post_task_type` varchar(50)   COMMENT '下游任务类型',
  `post_task_version` int(11)   COMMENT '下游任务版本',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='调度系统依赖节点的关系数据'
;
CREATE INDEX idx_project_dag_task ON t_ds_dag_task_relation_dep_data_df(pre_project_code, pre_dag_code, pre_task_code);

-- 清洗dep的关系数据
insert into t_ds_dag_task_relation_dep_data_df(pre_project_code,pre_dag_code,pre_task_code,post_project_name,post_project_code,post_dag_name,post_dag_code,post_dag_version,post_task_code,post_task_name,post_task_type,post_task_version)
select
JSON_EXTRACT(dep,'$.projectCode') as pre_project_code,
JSON_EXTRACT(dep,'$.definitionCode') as pre_dag_code,
JSON_EXTRACT(dep,'$.depTaskCode') as pre_task_code,
project_name as post_project_name,
project_code as post_project_code,
dag_name as post_dag_name,
dag_code as post_dag_code,
dag_version as post_dag_version,
task_code as post_task_code,
task_name as post_task_name,
task_type as post_task_type,
task_version as post_task_version
from (
      SELECT
         project_name,
         project_code,
         dag_name,
         dag_code,
         dag_version,
         task_code,
         task_version,
         task_name,
         task_type,
         JSON_UNQUOTE(JSON_EXTRACT(deps,CONCAT('$[', t_ds_help_ids.id, ']'))) AS dep
  FROM (
    SELECT id,
           project_name,
           project_code,
           dag_name,
           dag_code,
           dag_version,
           task_code,
           task_version,
           task_name,
           task_type,
           JSON_EXTRACT(task_params,'$.dependence.dependTaskList[*].dependItemList[*]') as deps
    FROM t_ds_task_node_base_data
    where task_type  = 'DEPENDENT'
  ) as base_data
  JOIN t_ds_help_ids ON t_ds_help_ids.id < JSON_LENGTH(deps)
  WHERE JSON_VALID(deps) AND JSON_LENGTH(deps)
) as t1
;

-- 第二部分数据
insert into t_ds_dag_task_relation_data_df(pre_project_name,pre_project_code,pre_dag_name,pre_dag_code,pre_dag_version,pre_task_code,pre_task_name,pre_task_type,pre_task_version,post_project_name,post_project_code,post_dag_name,post_dag_code,post_dag_version,post_task_code,post_task_name,post_task_type,post_task_version)
select
 node.project_name as pre_project_name
,t2.pre_project_code as pre_project_code
,node.dag_name as pre_dag_name
,t2.pre_dag_code as pre_dag_code
,node.dag_version as pre_dag_version
,t2.pre_task_code as pre_task_code
,node.task_name as pre_task_name
,node.task_type as pre_task_type
,node.task_version  as pre_task_version
,t2.post_project_name as post_project_name
,t2.post_project_code as post_project_code
,t2.post_dag_name as post_dag_name
,t2.post_dag_code as post_dag_code
,t2.post_dag_version as post_dag_version
,t2.post_task_code as post_task_code
,t2.post_task_name as post_task_name
,t2.post_task_type as post_task_type
,t2.post_task_version as post_task_version
from t_ds_dag_task_relation_dep_data_df as t2
left JOIN t_ds_task_node_base_data as node
on t2.pre_project_code = node.project_code
and t2.pre_dag_code = node.dag_code
and t2.pre_task_code = node.task_code
where node.project_name is not null
;

-- 第三部分数据
insert into t_ds_dag_task_relation_data_df(pre_project_name,pre_project_code,pre_dag_name,pre_dag_code,pre_dag_version,pre_task_code,pre_task_name,pre_task_type,pre_task_version,post_project_name,post_project_code,post_dag_name,post_dag_code,post_dag_version,post_task_code,post_task_name,post_task_type,post_task_version)
select
pre_project_name,pre_project_code,pre_dag_name,pre_dag_code,pre_dag_version,pre_task_code,pre_task_name,pre_task_type,pre_task_version,post_project_name,post_project_code,post_dag_name,post_dag_code,post_dag_version,post_task_code,post_task_name,post_task_type,post_task_version
from (
select
 node.project_name as pre_project_name
,t2.pre_project_code as pre_project_code
,node.dag_name as pre_dag_name
,t2.pre_dag_code as pre_dag_code
,0 as pre_dag_version
,0 as pre_task_code
,'整个工作流' as pre_task_name
,'DAG' as pre_task_type
,0  as pre_task_version
,t2.post_project_name as post_project_name
,t2.post_project_code as post_project_code
,t2.post_dag_name as post_dag_name
,t2.post_dag_code as post_dag_code
,t2.post_dag_version as post_dag_version
,t2.post_task_code as post_task_code
,t2.post_task_name as post_task_name
,t2.post_task_type as post_task_type
,t2.post_task_version as post_task_version
from (
  select * from t_ds_dag_task_relation_dep_data_df where pre_task_code = 0
) as t2
left JOIN t_ds_task_node_base_data as node
on t2.pre_project_code = node.project_code
and t2.pre_dag_code = node.dag_code
) as t
group by pre_project_name,pre_project_code,pre_dag_name,pre_dag_code,pre_dag_version,pre_task_code,pre_task_name,pre_task_type,pre_task_version,post_project_name,post_project_code,post_dag_name,post_dag_code,post_dag_version,post_task_code,post_task_name,post_task_type,post_task_version
;
