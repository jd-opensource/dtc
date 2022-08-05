### 工作流程

数据生命周期管理服务data_lifecycle_manager（以下简称DLM）主要与agent进行通信，流程如下：
1) data_lifecycle_manager服务启动后，从配置文件获取配置项，相关的配置项有如下：

定义数据规则的mysql语句：通过该配置来构造查询数据的mysql语句
匹配到规则的处理类型：目前主要是delete操作
定义处理时机的时间规则：暂定是crontab格式的规则，来判断何时执行冷数据清理工作，默认为每日凌晨1时执行
单次查询的记录条数：查询冷数据时每次获取固定条数的记录，默认为10条

2) 当清理时机到达后，data_lifecycle_manager向agent发送查询冷数据的mysql语句，并得到查询结果

3) 根据第2）步返回的查询结果，依次发送删除数据的命令到agent服务

### 配置项

dtc.yaml文件：

```
data_lifecycle:
   single.query.count: 10 // 单次查询的记录条数
   rule.sql: 'status = 0' // 定义数据规则的mysql语句
   rule.cron: '00 01 * * * ?' // 定义处理时机的时间规则，采用croncpp的格式，见https://github.com/mariusbancila/croncpp
   lifecycle.dbname: 'data_lifecycle_database' // data_lifecycle表对应的库名，该表记录上次操作的数据对应的id、update_time等信息
   lifecycle.tablename: 'data_lifecycle_table' // data_lifecycle表对应的表名
```

table.yaml文件

```
DATABASE_CONF:
  database_name: dtc_opensource  // 业务数据对应的库名
  database_number: (1,1)
  database_max_count: 1
  server_count: 1
 
MACHINE1:
  database_index: 0
  database_address: 127.0.0.1:3306
  database_username: username
  database_password: password
 
TABLE_CONF:
  table_name: dtc_opensource  // 业务数据对应的表名
  field_count: 5
  key_count: 1
  TableNum: (1,100)
 
FIELD1:
  field_name: uid  // 业务数据对应的key field字段名
  field_type: 1
  field_size: 4
```

agent.xml文件

```
<? xml version="1.0" encoding="utf-8" ?>
<ALL>
  <VERSION value="2"/>
  <AGENT_CONFIG AgentId="1"/>
  <BUSINESS_MODULE>
    <MODULE Mid="1319" Name="test1" AccessToken="000013192869b7fcc3f362a97f72c0908a92cb6d" ListenOn="0.0.0.0:12001" Backlog="500" Client_Connections="900"
        Preconnect="true" Server_Connections="1" Hash="chash" Timeout="3000" ReplicaEnable="true" ModuleIDC="LF" MainReport="false" InstanceReport="false" AutoRemoveReplica="true" TopPercentileEnable="false" TopPercentileDomain="127.0.0.1" TopPercentilePort="20020">
      <CACHESHARDING  Sid="293" ShardingReplicaEnable="true" ShardingName="test">
        <INSTANCE idc="LF" Role="replica" Enable="false" Addr="127.0.0.1:20000:1"/>
        <INSTANCE idc="LF" Role="master" Enable="true" Addr="127.0.0.1:20015:1"/>
      </CACHESHARDING>
    </MODULE>
  </BUSINESS_MODULE>
<VERSION value="2" />
    <LOG_MODULE LogSwitch="0" RemoteLogSwitch="1" RemoteLogIP="127.0.0.1" RemoteLogPort="9997" />
</ALL>
```

在agent.xml文件中解析ListenOn字段，提取出agent进程监控的端口号，通过该端口号与agent进行通信。

### 表设计

建表语句为：

```
CREATE TABLE `data_lifecycle_table` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ip` varchar(20) NOT NULL DEFAULT '0' COMMENT '执行清理操作的机器ip',
  `last_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '上次删除的记录对应的id',
  `last_update_time` timestamp COMMENT '上次删除的记录对应的更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
```

当一个data_lifecycle_manager进程根据查询出的记录执行完操作后，需要执行update操作更新last_update_time的值为当前操作最后操作的记录id对应的更新时间。以此来保证其它data_lifecycle_manager进程不会重复处理同一条记录，同时多个data_lifecycle_manager进程也可以并行的执行操作。


