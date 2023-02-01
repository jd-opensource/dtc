![DTC](http://storage.360buyimg.com/bq-install/release/dtc_logo.png)
# DTC - Distributed Table Cache 分布式表缓存
[![ubuntu&gcc-4.9](https://github.com/DTC8/DTC/actions/workflows/ubuntu-20.04&gcc-4.9.yml/badge.svg)](https://github.com/DTC8/DTC/actions/workflows/ubuntu-20.04&gcc-4.9.yml)

## 概述
DTC是一个分布式表级缓存系统，可为数据库提供热点数据缓存支持，减少数据库的访问压力。
![DTC architecture](http://storage.360buyimg.com/bq-install/release/architecture.png)
DTC系统由以下组件组成:
* **Agent** - 提供Key一致性Hash路由、命令请求的服务；能够减少Cache的连接数，提升性能。
* **Dtcd** - 提供热点数据缓存、DB代理的服务。
* **Connector** - 此组件为缓存与持久化存储数据库（例如：MySql）之间提供连接和数据传输功能。

## 特征
* 为数据库提供保护机制：
  - 保护空节点，防止缓存穿透。
  - 提供永不过期的缓存数据，防止缓存击穿。
  - 数据源线程柔性可用，保护数据库有限的连接数。
  - 预估超时机制，减少无效数据库请求。
* 保障缓存和数据库的数据一致性：
  - write-through机制，保证缓存和数据库数据一致。
  - 栅栏机制，防止高并发情景下更新请求丢失。
* 缓存高性能：
  - 集成内存分配机制，避免频繁系统调用。
  - I/O多路复用机制，承接大并发请求量。
  - 多种数据结构模型，提高内存。
* 扩展性：
  - 缓存节点横向扩容，增强缓存容量。 
  - 缓存节点纵向扩容，支持备读，解决热key问题。
  - 支持分库分表，支持持久化存储的拓展。

## 性能
    单核CPU，单DTC实例环境下，可支持90,000 QPS的查询请求量；
    在真实的分布式场景中，DTC可提供超过3,000,000 QPS的查询服务，命中率在99.9%以上，且平均响应时长在200微秒(μs)以内。

## 场景一：内存缓存模式
  内存缓存模式又称CacheOnly模式，是数据仅存储在共享内存中，类似redis的功能，断电即丢失。但由于使用了共享内存，即使进程挂掉或者重启，在下次启动dtc之后，仍然能够访问到存储的数据。
  客户端不需要特定的sdk，使用mysql-cli命令行便可访问dtc，并支持兼容类似mysql的增删改查命令。目前也已支持c++和python通过mysql的sdk访问。
  目前支持的sdk有
| 语言    | SDK            | 备注          |
|-------|----------------|-------------|
| Shell | mysql5.7       | mysql的官方命令行，mysql8.0仍有bug正在解决，暂不支持 |
| C++   | libmysqlclient | C++的常用库     |
| Python   | pymysql | python常用的mysql库，代码详见tests/test_dtcd_cache_only.py     |


 ### 1、配置文件
 #### 1.1   dtc.yaml
  dtc.yaml文件是dtc的主要配置文件，这里可以配置缓存的表结构、内存大小、监听端口等。
  下载解压缩安装包，新建dtc的配置文件dtc.yaml，文件位置在dtc/conf：

```
# 
# DTC configure file. v2
# Cache Only Test cases.
#
props:
  log.level: debug
  listener.port.dtc: 20015
  shm.mem.size: 100 #MB

primary:
  db: dtc
  table: &table opensource
  cache:
    field:
      - {name: &key uid, type: signed, size: 4}
      - {name: name, type: string, size: 50}
      - {name: city, type: string, size: 50}
      - {name: sex, type: signed, size: 4}
      - {name: age, type: signed, size: 4}
```

配置字段解释：
- props.log.level：日志输出等级，已弃用，现在在log4cplus.conf中配置
- props.listener.port.dtc：dtc监听的端口
- props.shm.mem.size：开辟的共享内存的大小，用于存储缓存数据，单位MB
- primary.db：数据库的名称，在CacheONLY模式下，只在sql的db字段有用，例如：select uid from dbname.tablename;
- primary.table: 缓存表的名称，在撰写sql语句时，需要用到，示例上；
- primary.cache: 配置文件中需重点关注的部分，用于设计缓存表的结构
- primary.cache.field：数组类型，每一个节点代表一个field字段；
- primary.cache.field[].name：字段名
- primary.cache.field[].type: 字段的类型，可以设置为signed(有符号数字类型），unsigned（无符号数字类型），string(字符串类型），binary（大小写敏感字符串类型）
- primary.cache.field[].size: 字段的长度，比如int signed可以设置为4/8等，string类型的size可以设置为字符串允许的最大长度。

 #### 1.2 log4cplus.conf
    此配置文件是日志的配置文件，依赖于log4cplus组件，配置和log4cplus相同。可以配置日志的打印级别、日志的输出目录和名称等。

 #### 1.3 my.conf
    此文件配置dtc的编码形式，当前默认的配置文件中是default-character-set=utf8，显示utf8的编码，如需其他编码类型，可以自行修改。

 ### 2、启动服务
    启动脚本是dtc/bin/dtc.sh，参数为stop | start | restart。
    启动进程执行：
    `./dtc.sh start`
    停止进程执行：
    `./dtc.sh stop`

    如果启动成功，可以在打印监听端口的时候，显示到监听端口。
![输入图片说明](https://foruda.gitee.com/images/1675238908292068772/b564ded8_2156244.png "屏幕截图")    

    如果启动成功，也可以在进程中看到两个dtcd服务。其中一个是父进程用于watchdog，另一个是真正的dtc业务
![输入图片说明](https://foruda.gitee.com/images/1675239014354077089/b7534d32_2156244.png "屏幕截图")

    如果未执行成功，请到dtc/log中查看错误日志，并寻求解决办法。

 ### 3、连接DTC
    使用mysql的客户端命令行便可连接访问dtc服务,如未安装，可以执行以下命令安装：
    ubuntu:
    `apt install mysql-client`
    注意当前只支持mysql5.X版本，mysql8.0版本的客户端存在兼容性问题正在解决。

    执行`mysql -h127.0.0.1 -P20016`即可像访问mysq一样，访问dtc服务：
    mysql -h和-P是mysql客户端的标准参数，分别制定ip地址和服务的port端口，如连接成功将如下图所示
![输入图片说明](https://foruda.gitee.com/images/1675239723356670339/c77e087b_2156244.png "屏幕截图")

 ### 4、操作DTC
 #### 4.1 mysql命令行
 ##### 4.1.1 写缓存
可以像在mysq中插入数据一样执行insert的sql来插入缓存：

```
insert into opensource(uid, name, city, sex, age) values(2, "kfysck", "shanghai", 1, 18);
```
当前只支持上面这种插入的语法，当插入成功之后就会显示以插入一行数据

![输入图片说明](https://foruda.gitee.com/images/1675242013748245302/d6eb6f8a_2156244.png "屏幕截图")

 ##### 4.1.2 读缓存
    和mysql一样，通过select的sql来读取缓存中的数据：
```
select uid,name,city,sex,age from opensource where uid = 2;
```
![输入图片说明](https://foruda.gitee.com/images/1675241969059211509/c2e15c9d_2156244.png "屏幕截图")

同时也支持通配符*星号：
```
select * from opensource where uid = 2;
```
![输入图片说明](https://foruda.gitee.com/images/1675242078426717447/a666b07e_2156244.png "屏幕截图")

 #### 4.2 Python SDK
    支持python常用的pymysql库，测试用例详见tests/test_dtcd_cache_only.py
 ##### 4.2.1 写缓存
```
import pymysql

db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
cursor = db.cursor()
sql = "insert into opensource(uid, name) values(1, 'hello')"
cursor.execute(sql)
db.commit()
rowsaffected = cursor.rowcount
print("affected rows: %s" % (rowsaffected))
cursor.close()
db.close()
```

 ##### 4.2.2 读缓存

```
import pymysql

db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
cursor = db.cursor()
sql = "select uid, name from opensource where uid = 1"
cursor.execute(sql)
results = cursor.fetchall()
for row in results:
    uid = row[0]
    name = row[1]
    print("uid=%s, name=%s" % (uid, name))
db.close()
```


## 社区
  在使用过程中遇到问题，或有好的意见建议，请提交[Issues](https://gitee.com/jd-platform-opensource/dtc/issues)。<br/>
  欢迎贡献代码，提交[Pull Requests](https://github.com/DTC8/DTC/pulls)。

## 第三方依赖
* [cJson](https://github.com/DaveGamble/cJSON)
* [log4cplus](https://github.com/log4cplus/log4cplus)
* [rocksdb](https://github.com/facebook/rocksdb)
* [yaml-cpp](https://github.com/jbeder/yaml-cpp)
* [zlib](https://zlib.net/)
* [gflags](https://github.com/gflags/gflags)
* [mxml](https://www.msweet.org/mxml/)
* [twemproxy](https://github.com/twitter/twemproxy)

## 项目成员
- [付学宝](https://gitee.com/fuxuebao)（项目发起者、导师、总设计师）
- [林金明](https://gitee.com/shrewdlin)（项目开发）
- [仇路](https://gitee.com/qiuluAbel)（项目开发）
- [杨爽](https://gitee.com/kfysck)（项目开发）
- [朱林](https://gitee.com/leol3)（项目开发）
- [陈雨杰](https://gitee.com/chenyujie28)（项目开发）
- [吴昕臻](https://gitee.com/wuxinzhen_1997)（项目开发）
- [曹沛](https://gitee.com/warm-byte)（项目开发）

## 特别感谢
感谢京东副总裁王建宇博士给予项目的大力支持，多次参与指导提供建议和方向！

## 许可证

京东集团 版权所有 © Copyright 2021-2023 [JD.com](https://www.jd.com), Inc.


许可证遵循 [Apache 2.0 协议](http://www.apache.org/licenses/LICENSE-2.0). 更多细节请访问 [LICENSE](./LICENSE).
