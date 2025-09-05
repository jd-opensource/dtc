## Directory Structure
Binary file directory: /usr/local/dtc<br/>
Configuration file directory: /usr/local/dtc/conf/<br/>
Log directory: /usr/local/dtc/log/<br/>
Statistics data directory: /usr/local/dtc/stat/<br/>
Binlog data directory: /usr/local/dtc/log/<br/>

## DTC Modes

DTC has two data modes: CACHE ONLY mode and Datasource mode.
- CACHE ONLY mode uses DTC as a cache without connecting to a database.
- Datasource mode requires a database connection and currently supports MySQL. In this mode, DTC acts as a database cache proxy, providing database and table sharding, and caching hot data in DTC.
  
The demo uses CACHE ONLY mode for demonstration.

## Table Structure
The table structure file is in conf/dtc.yaml.<br/>
The table name defined in the demo is dtc_opensource, <br/>
Structure:
| Field Name | Type                     | Length  |
| ---------- | ------------------------ | ------- |
| uid        | Integer                  | 4 Byte  |
| name       | String (case insensitive)| 50 Byte |
| city       | String (case insensitive)| 50 Byte |
| sex        | Integer                  | 4 Byte  |
| age        | Integer                  | 4 Byte  |

## Starting DTC Server
To save the trouble of environment configuration, the demo provides a docker image that can be run directly to start the server:<br/>
  ```shell
  docker pull dtc8/dtc:latest
  docker run --rm --name dtc -p <MY_LISTENER_PORT>:12001 -v <MY_HOST_CONF_DIR>:/usr/local/dtc/conf/ -e DTC_BIN=dtc -e DTC_ARGV=-ayc dtc8/dtc
  ```

## Running Client Test Examples
Currently supports MySQL 5.X and 8.X client access to DTC for SQL operations. After running the above docker, you can run the following SQL statements:
* Login:
```
  mysql -h127.0.0.1 -P12001 -uroot -proot
```
* View database list
```
  show databases;
```
* Switch database
```
  use layer2;
```
* View table list
```
  show tables;
```
* Insert
```
  insert into opensource(uid, name) values(1, 'Jack') where uid = 1;
```
* Update
```
  update opensource set name = 'Lee' where uid = 1;
```
* Query
```
  select uid, name from opensource where uid = 1;
```
* Delete
```
  delete from opensource where uid = 1;
```

You can also try modifying the code or configuration in the examples as needed for more experimentation. For configuration files, please refer to [Configure](./configure.md).

For source code compilation, please refer to [Building](./building.md).

## Direct Deployment
* Create directories
```
mkdir -p basepath
mkdir -p /usr/local/dtc/data
mkdir -p /usr/local/dtc/stat
mkdir -p /usr/local/dtc/log
mkdir -p /usr/local/dtc/conf
```
* Copy binary files to /usr/local/dtc directory and grant execution permissions
```
cp * /usr/local/dtc/
chmod +x *
```
* Run dtc
Execute ./dtc -h to get detailed information. You can run different components separately as needed. Note: The core module needs to run with root privileges.
```
  -h, --help                            : this help
  -v, --version                         : show version and exit
  -a, --agent                           : load agent module
  -c, --core                            : load dtc core module
  -l, --data-lifecycle                  : load data-lifecycle module
  -s, --sharding                        : load sharding module
  -r, --recovery mode                   : auto restart when crashed
```
Examples:
1. Run only core, without using agent proxy:
```
./dtc -c
```
2. Run DTC mode with agent proxy:
```
./dtc -ac
```
3. Run tiered storage
```
./dtc -ayc
```
4. Run tiered storage with database and table sharding
```
./dtc -aycs
```