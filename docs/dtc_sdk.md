#### 一、头文件

​	只需要`/dtc/src/devel/cpp/dtcapi.h`这一个头文件。

#### 二、动态库编译

​	`libdtc.2.so`，在`/dtc/build/src/devel/cpp`中生成。

#### 三、使用动态库

​	1、直接将动态库放置在默认的动态库目录`/usr/lib`下，并创建软连接：

​		`ln -s libdtc.2.so libdtc.so`

​		在编译的时候就可以直接使用`-ldtc`链接动态库。

​	2、将动态库放置在任意目录，并创建软连接：

​		`ln -s libdtc.2.so libdtc.so`

​		在编译的时候使用“-L 动态库绝对路径或者相对路径”，链接动态库。

#### 四、异常处理

​	除了`bad_alloc`内存分配失败外，调用`API`可能会发生的所有错误都会以错误码的方式返回。

#### 五、Server类

1、`set_address(const char *host, const char *port)` ：`设置dtc的ip和port`

```c++
server.set_address("127.0.0.1", "20015");
```

2、`set_timeout(int n) `：设置网络超时时间，单位为`ms`

```c++
server.set_timeout(5000);
```

3、`set_table_name(const char *name) `：设置`dtc`的表名

```c++
server.set_table_name("dtc_opensource");
```

4、`int_key(void) `、`string_key(void)` 、`binary_key(void)` ：声明KEY的类型

```c++
server.int_key();
```

5、`get_field_type(const char* name) `：根据字段名返回字段类型

```c++
int i = server.get_field_type("uid");
//`signed = 1`, `unsigned = 2`, `floatpoint  = 3`, `string = 4`, `binary = 5`
```

6、`get_address(void)`：返回服务器地址

```c++
printf("ip_address: %s\n", server.get_address());
```

7、`get_table_name(void)`：返回表名

```c++
printf("tablename: %s\n", server.get_table_name());
```

#### 六、Request类

1、`Request(Server *srv, int op)`：类似于一个`SQL`语句，是一个可处理的操作和参数。有以下几种特定操作的子类。

​	`GetRequest`：该类对指定`KEY`进行查询操作。

​	`InsertRequest`：该类对指定`KEY`进行插入操作。

​	`DeleteRequest`：该类对指定`KEY`进行删除一行记录操作。

​	`UpdateRequest`：该类对指定`KEY`进行更新操作。

​	`PurgeRequest`：该类对指定`KEY`进行删除所有记录操作。

​	`	ReplaceRequest`：该类对指定`KEY`进行操作，如果该KEY有多条记录，则把这多条记录替换为1条；如果该KEY无记录，则插入一条记录。

```c++
DTC::GetRequest GetReq(&server);
DTC::InsertRequest InsertReq(&server);
```

2、`set_key(t a)`：设定操作的KEY。可以设置多次，后一次设置覆盖前一次的值。

```c++
GetReq.set_key(1);
```

3、`need(const char *name) `：设置需要`select`的字段。**只能在`GetRequest`里使用**。

```c++
GetReq.need("uid");
```

4、`do_execute(Result &s)`：提交请求，执行结果被保存在Result对象里

```c++
GetReq.do_execute(result);
```

5、`int Set(const char *, long long)`、`int Set(const char *, double)`、`int Set(const char *, const char *)`：设置KEY-VALUE值。`InsertRequest`只能用`Set`。

```c++
UpdateReq.Set("age", 12);
InsertReq.Set("uid", 1);
```

6、`int Add(const char *, long long)`、`int Add(const char *, double)`、`Sub(const char *, long long)`、`Sub(const char *, double)`：更新相应字段`VALUE`值

```c++
UpdateReq.Add("age", 8);
UpdateReq.Sub("age", 1);
```

7、`limit(unsigned int, unsigned int)`：返回结果集开始位置，以及所需要行数。类似于`SQL`中的`limit`。

```c++
get_request.limit(2, 2);
```

8、`EQ/NE/LT/GT/LE/GE(const char *fieldname, value)`：设定除KEY外的额外条件，多个条件为and关系。

```c++
/*    
EQ		NE		LT		GT		LE		GE
=		!=		<		>		<=		>=
*/
UpdateReq.LT("age",15);
```

#### 七、Result类

1、`get_error_from(void)`：返回错误阶段

```c++
result.get_error_from();
```

2、`get_error_message(void)`：返回错误信息

```c++
result.get_error_message();
```

3、`get_num_row_size(void)`：返回执行结果行数

```c++
//如果数据不存在
if (result.get_num_row_size() <= 0)
		printf("uin[%u] data not exist.\n", uid);
```

4、`int_key(void)`：读取结果的`KEY`值

```c++
printf("result key: %lld\n", result.int_key());
```

5、`fetch_row(void)`：读取一行数据。

```c++
result.fetch_row();
```

6、`int_value(const char *)`，`binary_value(const char *)`，`string_value(const char *)`、`float_value(const char *)`：读取特定字段内容

```c++
printf("uid: %lld\n", result.int_value("uid"));
printf("age:%lld\n", result.int_value("age"));
printf("city: %s\n", result.string_value("city"));
```

7、`get_affected_rows_size(void)`：返回修改影响的行数

```c++
printf("affected rows size %d\n", stResult.get_affected_rows_size());
```

