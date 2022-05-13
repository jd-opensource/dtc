* gcc/g++ 4.8
* cmake
* 安装gflags:<br />
    gflags是google开源的一套命令行参数解析工具，支持从环境变量和配置文件读取参数：
    ```shell
    git clone https://github.com/gflags/gflags.git
    cd gflags
    git checkout -b 2.2 v2.2.2
    cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DBUILD_SHARED_LIBS=ON -DGFLAGS_NAMESPACE=google -G "Unix Makefiles" .
    make && sudo make install
    sudo ldconfig
    sudo ln -s /usr/local/lib/libgflags.so.2.2 /lib64
    ```
    ### CentOS
    - 执行以下命令安装依赖：
    ```shell
    sudo yum install -y make snappy snappy-devel zlib zlib-devel bzip2 bzip2-devel lz4-devel libasan openssl-devel cmake3 mysql-devel mxml-devel
    ```
    ### Ubuntu
    - 执行以下命令安装依赖：
    ```shell
    sudo apt-get install make snappy libsnappy-dev zlib1g zlib1g-dev bzip2 liblz4-dev libasan0  openssl libmxml-dev
    ```
* `mkdir build`
* `cmake ../`
  * 默认编译配置不包含测试用例，如需要编译test文件夹下的测试用例，需要再cmake时添加参数 -DCMAKE_TEST_OPTION=ON
    ```shell
    cmake -DCMAKE_TEST_OPTION=ON ../
    ```
* `make`
* `make install`