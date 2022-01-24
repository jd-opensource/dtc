![DTC](http://storage.360buyimg.com/bq-install/release/dtc_logo.png)
# DTC - Distributed Table Cache 分布式表缓存
[![Build Status](https://app.travis-ci.com/DTC8/DTC.svg?branch=master)](https://app.travis-ci.com/github/DTC8/DTC)

## Overview
DTC is a high performance Distributed Table Cache system designed by JD.com that offering hotspot data cache for databases in order to reduce pressure of database and improve QPS.

![](http://storage.360buyimg.com/bq-install/release/architecture.png)

The DTC system consists of the following components:
* **Agent** - Provides key consistent hash routing in order to reduce connections and improve performance.
* **Dtcd** - Provides hot data caching service.
* **Connector** - Provides connection and communication between cache and persistent storage database such as MYSQL.

## Feature
* Database Protection
  - protection for null node, prevent cache breakdown.
  - provide long-term data caching, and prevent cache penetration.
  - data source thread available, protect the database with a limited number of connections.
  - Estimated timeout policy to reduce invalid database requests.
* Data consistency
  - write-through policy, ensure cache and database data consistent.
  - barrier policy to prevent update requests lost while concurrcy.
* Performance
  - integrated memroy allocation policy to avoid frequent system calls.
  - I/O multiplexing to handle concurrcy requests.
  - multiple data structure models to improve memory performance.
* Scalability
  - cache node expands horizontally to enhance cache capacity.
  - cache node expands vertically, supports slave reading, and solve the bottleneck of hot keys.
  - provide sharding, supports for persistent storage scalable.
## Performance
* DTC can process 90,000 QPS of query requests at single-core cpu & single dtc instance.
* DTC can provide above 3,000,000 QPS query capability with above 99.9% hit rate and less than 200 μs response time in actual distributed scenarios.
## How to Build
DTC provides docker images for quick start:
* Start server docker:<br/>
```shell
docker pull dtc8/server:latest
docker run -i -t --name dtc-server -p 127.0.0.1:20015:20015 dtc8/server:latest
```

For more compile information, see [Building](docs/building.md).<br/>
Trying a demo, visit [QuickStart](docs/quickstart.md).

## License
JD.com © Copyright 2022 [JD.com](https://ir.jd.com/), Inc.<br/>
[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0). visit for more details [LICENSE](./LICENSE).