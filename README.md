![DTC](http://storage.360buyimg.com/bq-install/release/dtc_logo.png)
# 编译
## 编译依赖包
### Ubuntu
* gcc 4.8
* g++ 4.8
* cmake >= 3.6.2
* 执行以下命令安装依赖：<br />
    `sudo apt-get install snappy libsnappy-dev zlib1g zlib1g-dev bzip2 liblz4-dev libasan0  openssl libmxml-dev`<br />
* 安装gflags:<br />
    gflags是google开源的一套命令行参数解析工具，支持从环境变量和配置文件读取参数<br />
    `git clone https://github.com/gflags/gflags.git`<br />
    `cd gflags`<br />
    `git checkout -b 2.2 v2.2.2`<br />
    `cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DBUILD_SHARED_LIBS=ON -DGFLAGS_NAMESPACE=google -G "Unix Makefiles" .`<br />
    `make && sudo make install`<br />
    `sudo ldconfig  `<br />
    `sudo ln -s /usr/local/lib/libgflags.so.2.2 /lib64`<br />
### CentOS7
* gcc/g++ 4.8
* cmake >= 3.6.2
* 执行以下命令安装依赖：<br />
    `sudo yum install -y snappy snappy-devel zlib zlib-devel bzip2 bzip2-devel lz4-devel libasan openssl-devel cmake3 mysql-devel mxml-devel`<br />
* 安装gflags:<br />
    gflags是google开源的一套命令行参数解析工具，支持从环境变量和配置文件读取参数<br />
    `git clone https://github.com/gflags/gflags.git`<br />
    `cd gflags`<br />
    `git checkout -b 2.2 v2.2.2`<br />
    `cmake3 -DCMAKE_INSTALL_PREFIX=/usr/local -DBUILD_SHARED_LIBS=ON -DGFLAGS_NAMESPACE=google -G "Unix Makefiles" .`<br />
    `make && sudo make install`<br />
    `sudo ldconfig  `<br />
    `sudo ln -s /usr/local/lib/libgflags.so.2.2 /lib64`<br />

### 3rd-party Dependence
* [cJson](https://github.com/DaveGamble/cJSON)
* [log4cplus](https://github.com/log4cplus/log4cplus)
* [rocksdb](https://github.com/facebook/rocksdb)
* [yaml-cpp](https://github.com/jbeder/yaml-cpp)
* [zlib](https://zlib.net/)
* [gflags](https://github.com/gflags/gflags)
* [mxml](https://www.msweet.org/mxml/)