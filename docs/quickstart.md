## 目录结构
Bin文件目录：/usr/local/dtc<br/>
配置文件目录：/usr/local/dtc/conf/<br/>
日志目录：/usr/local/dtc/log/<br/>
统计数据目录：/usr/local/dtc/stat/<br/>
binlog数据目录：/usr/local/dtc/log/<br/>

## DTC模式

DTC分为两种数据模式：CACHE ONLY模式和Datasource模式。
- CACHE ONLY模式的DTC当做缓存使用，不连接数据库。
- Datasource模式需要连接数据库，目前支持连接Mysql。此模式下DTC作为数据库的缓存代理，提供分库分表，将热点数据缓存在DTC中。
  
Demo使用的是CACHE ONLY模式演示。

## 表结构
表结构文件在conf/dtc.yaml中。<br/>
demo中定义的表名为dtc_opensource, <br/>
结构为：
| 字段名 | 类型                   | 长度    |
| ------ | ---------------------- | ------- |
| uid    | 整型                   | 4 Byte  |
| name   | 字符串（大小写不敏感   | 50 Byte |
| city   | 字符串（大小写不敏感） | 50 Byte |
| sex    | 整型                   | 4 Byte  |
| age    | 整型                   | 4 Byte  |

## 启动DTC Server端
为了省去配置环境的麻烦，Demo中提供docker镜像，直接运行即可启动服务端：<br/>
  ```shell
  docker pull dtc8/dtc:latest
  docker run --rm --name dtc -p <MY_LISTENER_PORT>:12001 -v <MY_HOST_CONF_DIR>:/usr/local/dtc/conf/ -e DTC_BIN=dtc -e DTC_ARGV=-ayc dtc8/dtc
  ```

## 运行Client测试示例
当前已经支持mysql 5.X和8.X的客户端访问dtc进行SQL操作。当运行上面docker之后，可以运行以下SQL语句：
* 登录：
```
  mysql -h127.0.0.1 -P12001 -uroot -proot
```
* 查看数据库列表
```
  show databases;
```
* 切换数据库
```
  use layer2;
```
* 查看表列表
```
  show tables;
```
* 插入
```
  insert into opensource(uid, name) values(1, 'Jack') where uid = 1;
```
* 更新
```
  update opensource set name = 'Lee' where uid = 1;
```
* 查询
```
  select uid, name from opensource where uid = 1;
```
* 删除
```
  delete from opensource where uid = 1;
```

你也可以根据需要尝试修改示例中的代码或配置，进行更多的体验。配置文件请参考[Configure](./configure.md)。

源码编译请参照[buiding](./building.md)。

## 直接部署
* 创建文件夹
```
mkdir -p basepath
mkdir -p /usr/local/dtc/data
mkdir -p /usr/local/dtc/stat
mkdir -p /usr/local/dtc/log
mkdir -p /usr/local/dtc/conf
```
* 将bin文件拷贝到/usr/local/dtc文件夹，并赋执行权限
```
cp * /usr/local/dtc/
chmod +x *
```
* 运行dtc
执行./dtc -h获取详细信息，可以依据需要分别运行不同的组件。注：需要在root权限下运行core模块。
```
  -h, --help                            : this help
  -v, --version                         : show version and exit
  -a, --agent                           : load agent module
  -c, --core                            : load dtc core module
  -l, --data-lifecycle                  : load data-lifecycle module
  -y, --async-conn                 : load async-conn module
  -s, --sharding                        : load sharding module
  -r, --recovery mode                   : auto restart when crashed
```
例如：
1.只运行core，不使用agent代理：
```
./dtc -c
```
2.运行agent代理的dtc模式：
```
./dtc -ac
```
3.运行分层存储
```
./dtc -ayc
```
4.运行带有分库分表的分层存储
```
./dtc -aycs
```