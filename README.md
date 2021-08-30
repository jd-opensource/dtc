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
  - 支持分库分表。
* 缓存高性能：
  - 集成内存分配机制，避免频繁系统调用。
  - I/O多路复用机制，承接大并发请求量。
  - 多种数据结构模型，提高内存读写速度。

## 性能
    单核CPU，单DTC实例情况下，可支持90,000 QPS的查询；
    在真实的分布式场景中，DTC可支持超过3,000,000 QPS的查询服务，且命中率在99.9%以上。

## 构建
  项目提供docker镜像，可快速启动和运行示例：
  - 启动server镜像：<br/>
  `docker pull dtc8/server:latest`<br/>
  `docker run -i -t -p 127.0.0.1:20015:20015 dtc8/server:latest`
  - 启动SDK镜像：
    - C++:<br/>
    `docker pull dtc8/sdk-cpp:latest`<br/>
    `docker run -i -t dtc8/sdk-cpp:latest /bin/bash`
    - 后续计划SDK支持更多语言版本。

  更多编译详情，请移步[buiding](docs/buiding.md).

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
- 付学宝（项目发起者、导师、总设计师）
- [林金明](https://gitee.com/shrewdlin)（项目开发）
- [杨爽](https://gitee.com/kfysck)（项目开发）
- [仇路](https://gitee.com/qiuluAbel)（项目开发）
- [吴昕臻](https://gitee.com/wuxinzhen_1997)（项目开发）
- [曹沛](https://gitee.com/warm-byte)（项目开发）
- [陈雨杰](https://gitee.com/chenyujie28)（项目开发）
- [朱林](https://gitee.com/leol3)（项目开发）

## 许可证

京东集团 版权所有 © Copyright 2021 [JD.com](https://www.jd.com), Inc.


许可证遵循 [Apache 2.0 协议](http://www.apache.org/licenses/LICENSE-2.0). 更多细节请访问 [LICENSE](https://gitee.com/jd-platform-opensource/dtc/blob/master/LICENSE).