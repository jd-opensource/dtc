
## 配置文件
配置文件目录：/etc/dtc<br/>
- AGENT服务的配置文件是：
  * agent.xml 指定后端dtc server的主、备服务的地址信息和权重；
- DTC服务的配置文件是：
  * dtc.yaml 配置缓存模式，缓存大小等。
  * table.yaml 表结构的定义、数据库连接信息在此配置。

下面就各配置文件具体配置选项做介绍：
### agent.xml
agent配置文件位置为当前项目的conf/agent.xml，主要配置以下字段：
- ALL.BUSINESS_MODULE.MODULE.ListenOn 监听IP和端口号
- ALL.BUSINESS_MODULE.MODULE.Preconnect 预连接状态
  * True 开启预连接状态，当Agent启动时会自动建立对后端所有DTC Server结点的连接通道。
  * False 关闭预连接状态，Agent启动时并不会针对后端Server建立连接，待第一个请求过来时才会创建连接。
- ALL.BUSINESS_MODULE.MODULE.Timeout 单位：毫秒。Agent响应超时时间，超过此时间则直接返回。
- ALL.BUSINESS_MODULE.CACHESHARDING 此节点为Agent的分片配置信息。根据分片服务器的数量，可配置多个CACHESHARDING节点。
- ALL.BUSINESS_MODULE.CACHESHARDING.INSTANCE 分片节点下的具体服务器实例信息：
  * Role 实例角色配置：
    *  master 主服务器角色设置，至少要配置一个。
    *  replica [可选] 备服务器角色设置，可不配置或配置多个备机。
  * Enable 角色开关：True为开启，False为关闭。
  * Addr 服务器监听地址和权重。例如 0.0.0.0:20015:1, 分别代表了监听的IP、监听的端口和权重值。权重值在INSTANCE实例下的所有服务器的请求权重，Agent会根据此值设置的大小，来分发不同的流量到后端DTC Server上。默认值为1。


### dtc.yaml
cache配置文件位置为当前项目的conf/dtc.yaml，主要配置以下字段：
* DTCID 整形数字值。缓存在系统当中的标识。需保证在同一台服务器上，每个DTC Server的实例此值唯一。
* MAX_USE_MEM_MB 整形数字值，单位为MB。设置此实例可提供的缓存大小。单台服务器上所有实例申请内存的值的总和要小于此服务器物理内存的最大值。
* DTC_MODE 0或1，默认值为1.设置为1时，使用CACHE ONLY模式；设置为0时，使用的是DB CACHE模式。
* LOG_LEVEL 可配置debug/info/error等日志输出级别。

### table.yaml
cache配置文件位置为当前项目的conf/table.yaml，主要配置以下字段：
* TABLE_CONF.table_name
* TABLE_CONF.field_count
* TABLE_CONF.key_count 指定
* FIELD*ID* *ID*为当前字段的编号，从1开始使用，可以根据场景需要配置个数。
  * FIELD*ID*.field_name 字段名
  * FIELD*ID*.field_type 字段类型：1.整数型 2.无符号整数形 3.浮点型 4.字符串（大小写不明感） 5.字符串（大小写敏感）
  * FIELD*ID*.field_size 字段长度。定义了该字段的大小。例如int型可配置为4字节，long型可配置为8字节，字符串类型可根据具体使用场景配置长度，但最大长度不得超过64KB。
  * FIELD*ID*.field_unique [可选]默认值0，可配置0或1。当为0时表示该字段的值不唯一，1时表示该字段值唯一。