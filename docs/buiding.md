* gcc/g++ 4.9
* cmake
* Install gflags:<br />
    gflags is an open-source command-line flag parsing tool from Google that supports reading parameters from environment variables and configuration files:
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
    - Execute the following command to install dependencies:
    ```shell
    sudo yum install -y make snappy snappy-devel zlib zlib-devel bzip2 bzip2-devel lz4-devel libasan openssl-devel cmake3 mysql-devel mxml-devel
    ```
    ### Ubuntu
    - Execute the following command to install dependencies:
    ```shell
    sudo apt-get install make snappy libsnappy-dev zlib1g zlib1g-dev bzip2 liblz4-dev libasan0  openssl libmxml-dev
    ```
* `mkdir build`
* `cmake ../`
  * The default compilation configuration does not include test cases. If you need to compile test cases in the test folder, you need to add the parameter -DCMAKE_TEST_OPTION=ON when running cmake
    ```shell
    cmake -DCMAKE_TEST_OPTION=ON ../
    ```
* `make`
* `make install`