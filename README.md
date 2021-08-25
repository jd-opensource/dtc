![DTC](http://storage.360buyimg.com/bq-install/release/dtc_logo.png)
# DTC - Distributed Table Cache 分布式表缓存

## 概述
DTC是一个分布式表级缓存系统。

DTC系统由以下组件组成:
* **Agent** - 提供Key路由、命令请求的服务。
* **Dtcd** - 提供热点数据缓存服务。
* **Connector** - 此组件为缓存与持久化存储数据库（例如：MySql）之间提供连接和数据传输功能。

## 特征

## 性能
    单核CPU，单DTC实例情况下，可支持60,000 QPS的查询；
    在真实的分布式场景中，DTC可支持超过3,000,000 QPS的查询服务，且命中率在99.9%以上。

## 构建
* gcc/g++ 4.8
* cmake >= 3.6.2
* 安装gflags:<br />
    gflags是google开源的一套命令行参数解析工具，支持从环境变量和配置文件读取参数<br />
    `git clone https://github.com/gflags/gflags.git`<br />
    `cd gflags`<br />
    `git checkout -b 2.2 v2.2.2`<br />
    `cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DBUILD_SHARED_LIBS=ON -DGFLAGS_NAMESPACE=google -G "Unix Makefiles" .`<br />
    `make && sudo make install`<br />
    `sudo ldconfig  `<br />
    `sudo ln -s /usr/local/lib/libgflags.so.2.2 /lib64`<br />
    ### CentOS
    - 执行以下命令安装依赖：<br />
    `sudo yum install -y snappy snappy-devel zlib zlib-devel bzip2 bzip2-devel lz4-devel libasan openssl-devel cmake3 mysql-devel mxml-devel`<br />
    ### Ubuntu
    - 执行以下命令安装依赖：<br />
    `sudo apt-get install snappy libsnappy-dev zlib1g zlib1g-dev bzip2 liblz4-dev libasan0  openssl libmxml-dev`<br />
* src目录下，执行make即可编译。

## 第三方依赖
* [cJson](https://github.com/DaveGamble/cJSON)
* [log4cplus](https://github.com/log4cplus/log4cplus)
* [rocksdb](https://github.com/facebook/rocksdb)
* [yaml-cpp](https://github.com/jbeder/yaml-cpp)
* [zlib](https://zlib.net/)
* [gflags](https://github.com/gflags/gflags)
* [mxml](https://www.msweet.org/mxml/)
* [twemproxy](https://github.com/twitter/twemproxy)

## 许可证

京东集团 版权所有 © Copyright 2021 [JD.com](https://www.jd.com), Inc.


许可证遵循 [Apache 2.0 协议](http://www.apache.org/licenses/LICENSE-2.0). 更多细节请访问 [LICENSE](https://gitee.com/jd-platform-opensource/dtc/blob/master/LICENSE).