## 目录结构
Bin文件目录：/usr/local/dtc<br/>
配置文件目录：/etc/dtc<br/>
日志目录：/var/log/<br/>
统计数据目录：/usr/local/dtc/stat/<br/>
binlog数据目录：/usr/local/log/<br/>

## DTC模式

DTC分为两种数据模式：CACHE ONLY模式和DB CACHE模式。
- CACHE ONLY模式的DTC当做缓存使用，不连接数据库。
- DB CACHE模式需要连接数据库，目前支持连接Mysql。此模式下DTC作为数据库的缓存代理，将热点数据缓存在DTC中。
  
Demo使用的是CACHE ONLY模式演示。

## 表结构
表结构文件在conf/table.yaml中。<br/>
demo中定义的表名为dtc_opensource, <br/>
结构为：
| 字段名 | 类型                   | 长度    |
| ------ | ---------------------- | ------- |
| uid    | 整型                   | 4 Byte  |
| name   | 字符串（大小写敏感）   | 50 Byte |
| city   | 字符串（大小写不敏感） | 50 Byte |
| sex    | 整型                   | 4 Byte  |
| age    | 整型                   | 4 Byte  |

## 启动DTC Server端
为了省去配置环境的麻烦，Demo中提供docker镜像，直接运行即可启动服务端：<br/>
  ```shell
  docker pull dtc8/server:latest
  docker run -i -t --name dtc-server -p 127.0.0.1:20015:20015 dtc8/server:latest
  ```
如非首次运行容器，则有可能会提示容器已存在，删除旧容器即可：
  ```shell
  docker rm dtc-server
  ```

## 启动Agent端
  在docker环境中，agent和dtc-server需要在同一个网络环境中才能相互通信，故在启动时使用--network=container参数。
  ```shell
  docker pull dtc8/agent:latest
  docker run -i -t --name agent --network=container:dtc-server dtc8/agent:latest
  ```
如非首次运行容器，则有可能会提示容器已存在，删除旧容器即可：
  ```shell
  docker rm agent
  ```
## 运行Client测试示例
client测试示例在server容器当中，进入容器：
  ```shell
  docker exec -it dtc-server /bin/bash
  ```
进入示例所在目录：
  ```shell
  cd /usr/local/demo
  ```
此目录中有get和insert两个bin文件，对应的源代码在当前项目的test文件夹中。<br/>
运行insert即可插入一条数据
```shell
chmod +x insert
./insert
```
数据内容为：
| 字段名 | 值       | 备注 |
| ------ | -------- | ---- |
| uid    | 1        | KEY  |
| name   | norton   |      |
| city   | shanghai |      |
| age    | 18       |      |
| sex    | 1        |      |

运行get即可查询并打印出刚才插入的数据。
```shell
chmod +x get
./get
```
你也可以根据需要尝试修改示例中的代码或配置，进行更多的体验。配置文件请参考[Configure](./configure.md)。

源码编译请参照[buiding](./building.md)。