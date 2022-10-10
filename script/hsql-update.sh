sh download.sh
tar -zxvf hsql.tar.gz
cp -rf hsql/include/ ../src/libs/hsql/
cp -f hsql/libs/libsqlparser.so ../src/libs/hsql/libs/libsqlparser.so
cp -f hsql/libs/libsqlparser.a ../src/libs/hsql/libs/libsqlparser.a
rm -rf hsql/
rm -rf hsql.tar.gz