
## 配置文件
配置文件目录：conf/<br/>
- AGENT服务的配置文件是：
  * agent.xml 指定后端dtc server的主、备服务的地址信息和权重；
- DTC服务的配置文件是：
  * dtc.yaml dtc模块的配置文件，包括基础配置和表结构信息。

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
#### primary 主库，提供dtc核心功能
* table dtc的表名
* layered.rule 分层存储的命中规则，匹配到此规则就进入到cache层。
* cache/hot/full
  dtc根据不同的功能需要配置不同的模块，共分为三层：<br />
  cache层：提供缓存功能，只设置cache，不设置hot则表示CACHE ONLY模式，只缓存数据不存储数据到数据库。<br />
  hot层：在cache的基础上提供数据存储功能，在datasource模式下需要配置此层来设置具体的数据源信息。在分层存储时，设置此模块能够配置热点数据的数据源<br />
  full层：在分层存储的功能时，需要配置此层，全量数据将存储在此数据源中。
* logic/real：
该字段分别设置逻辑库表和真实库表。逻辑库表用于在dtc中显示和使用库、表。真实库表信息是真实的数据源信息。
* sharding:
该字段用户设置分库分表的信息，key字段用于设置依照此字段进行分片。table字段用于设置分表的信息，例如分表名为opensource_0/opensource_1.....opensouce_9，则只需要设置为{prefix: [*table, _], start: 0, last: 9}

#### table
cache配置文件位置为当前项目的conf/table.yaml，主要配置以下字段：
* TABLE_CONF.table_name
* TABLE_CONF.field_count
* TABLE_CONF.key_count 指定
* FIELD*ID* *ID*为当前字段的编号，从1开始使用，可以根据场景需要配置个数。
  * FIELD*ID*.field_name 字段名
  * FIELD*ID*.field_type 字段类型：1.整数型 2.无符号整数形 3.浮点型 4.字符串（大小写不明感） 5.字符串（大小写敏感）
  * FIELD*ID*.field_size 字段长度。定义了该字段的大小。例如int型可配置为4字节，long型可配置为8字节，字符串类型可根据具体使用场景配置长度，但最大长度不得超过64KB。
  * FIELD*ID*.field_unique [可选]默认值0，可配置0或1。当为0时表示该字段的值不唯一，1时表示该字段值唯一。

#### extension 扩展库，提供多租户功能
在dtc的基础缓存和数据代理功能之外，还提供了扩展库，通过配置此模块能够在数据库中进行复杂查询和分库分表功能。