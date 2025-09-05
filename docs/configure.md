
## Configuration Files
Configuration file directory: conf/<br/>
- AGENT service configuration file:
  * agent.xml Specifies the address information and weight of primary and backup services of backend DTC servers;
- DTC service configuration file:
  * dtc.yaml Configuration file for the DTC module, including basic configuration and table structure information.

The following sections introduce specific configuration options for each configuration file:
### agent.xml
The agent configuration file is located at conf/agent.xml in the current project. The main fields to configure are:
- ALL.BUSINESS_MODULE.MODULE.ListenOn Listen IP address and port number
- ALL.BUSINESS_MODULE.MODULE.Preconnect Pre-connection status
  * True Enable pre-connection status. When Agent starts, it will automatically establish connection channels to all backend DTC Server nodes.
  * False Disable pre-connection status. Agent will not establish connections to backend servers when starting, and will only create connections when the first request arrives.
- ALL.BUSINESS_MODULE.MODULE.Timeout Unit: milliseconds. Agent response timeout. If this time is exceeded, it will return directly.
- ALL.BUSINESS_MODULE.CACHESHARDING This node contains Agent's sharding configuration information. Multiple CACHESHARDING nodes can be configured based on the number of sharding servers.
- ALL.BUSINESS_MODULE.CACHESHARDING.INSTANCE Specific server instance information under the sharding node:
  * Role Instance role configuration:
    *  master Primary server role setting, at least one must be configured.
    *  replica [Optional] Backup server role setting, can be not configured or multiple backup servers can be configured.
  * Enable Role switch: True to enable, False to disable.
  * Addr Server listen address and weight. For example, 0.0.0.0:20015:1 represents the listen IP, listen port, and weight value respectively. The weight value is the request weight among all servers under the INSTANCE. Agent will distribute different traffic to backend DTC Servers based on the size of this value. Default value is 1.


### dtc.yaml
#### primary Primary database, provides DTC core functionality
* table DTC table name
* layered.rule Hit rules for layered storage. Requests matching this rule will enter the cache layer.
* cache/hot/full
  DTC needs to configure different modules based on different functional requirements, divided into three layers:<br />
  cache layer: Provides caching functionality. If only cache is set without hot, it represents CACHE ONLY mode, which only caches data without storing data to the database.<br />
  hot layer: Provides data storage functionality on top of cache. In datasource mode, this layer needs to be configured to set specific data source information. In layered storage, setting this module allows configuration of data sources for hot data<br />
  full layer: When using layered storage functionality, this layer needs to be configured. Full data will be stored in this data source.
* logic/real:
These fields set logical database tables and real database tables respectively. Logical database tables are used for display and usage of databases and tables in DTC. Real database table information is the actual data source information.
* sharding:
This field is used to set database and table sharding information. The key field is used to set which field to use for sharding. The table field is used to set table sharding information. For example, if table names are opensource_0/opensource_1.....opensource_9, you only need to set it as {prefix: [*table, _], start: 0, last: 9}

#### table
Cache configuration file is located at conf/table.yaml in the current project. The main fields to configure are:
* TABLE_CONF.table_name
* TABLE_CONF.field_count
* TABLE_CONF.key_count Specify
* FIELD*ID* *ID* is the field number, starting from 1. The number can be configured based on scenario requirements.
  * FIELD*ID*.field_name Field name
  * FIELD*ID*.field_type Field type: 1.Integer 2.Unsigned integer 3.Float 4.String (case insensitive) 5.String (case sensitive)
  * FIELD*ID*.field_size Field length. Defines the size of the field. For example, int type can be configured as 4 bytes, long type can be configured as 8 bytes. String type can be configured with length based on specific usage scenarios, but maximum length cannot exceed 64KB.
  * FIELD*ID*.unique [Optional] Default value 0, can be configured as 0 or 1. When 0, it means the field value is not unique; when 1, it means the field value is unique.

#### extension Extension library, provides multi-tenant functionality
In addition to DTC's basic caching and data proxy functionality, an extension library is also provided. By configuring this module, complex queries and database and table sharding functionality can be performed in the database.