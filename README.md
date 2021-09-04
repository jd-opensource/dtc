![DTC](http://storage.360buyimg.com/bq-install/release/dtc_logo.png)
# DTC - Distributed Table Cache 分布式表缓存
[![Build Status](https://app.travis-ci.com/DTC8/DTC.svg?branch=master)](https://app.travis-ci.com/github/DTC8/DTC)

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
  - 多种数据结构模型，提高内存
* 扩展性：
  - 缓存节点横向扩容，增强缓存容量。 
  - 缓存节点纵向扩容，支持备读，解决热key问题。
  - 支持分库分表，支持持久化存储的拓展。

## 性能
    单核CPU，单DTC实例环境下，可支持90,000 QPS的查询请求量；
    在真实的分布式场景中，DTC可提供超过3,000,000 QPS的查询服务，命中率在99.9%以上，且平均响应时长在200微秒(μs)以内。

## 构建
  提供docker镜像，可快速启动服务：
  - 启动server镜像：<br/>
  ```shell
  docker pull dtc8/server:latest
  docker run -i -t --name dtc-server -p 127.0.0.1:20015:20015 dtc8/server:latest
  ```

  更多编译详情，请移步[Buiding](docs/buiding.md).<br/>
  体验Demo，请移步[QuickStart](docs/queckstart.md).

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

京东集团 版权所有 © Copyright 2021 [JD.com](https://www.jd.com), Inc.


许可证遵循 [Apache 2.0 协议](http://www.apache.org/licenses/LICENSE-2.0). 更多细节请访问 [LICENSE](https://gitee.com/jd-platform-opensource/dtc/blob/master/LICENSE).