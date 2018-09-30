
# SHARKSTORE

[中文版本](README_CN.md)

SharkStore is a distributed, persistent key-value store, whose database layer is based on the `rocksdb` and the replication works are based on `raft`.

**Modules**


- master-server(golang)    --    metadata server
- data-server(c++)         --    database server
- gateway-server(golang)   --    proxy
- console(golang)          --    web administration


**Directory**

```
|-- README.md
|-- console                 web administration tool
|-- data-server             business data storage service, where data is stored
|-- glide.yaml
|-- master-server           metadata service, where you get metadata
|-- model
|-- pkg-go                  common module
|-- proxy
    |-- gateway-server      sql/restful proxy
```


# Architecture Design

See [arch.md](doc/arch.md) .



# Install

See [INSTALL.md](INSTALL.md) .



# Performance

We created a table named 'metric' witch 12 columns on 3 data servers with NVMe disk, and each of the  columns is integer type. The first four columns(named salt, key, host, ts) are made a unified index together as the key. On the other hand, the table is pre-splitted to 100 ranges with 3 replicas running on raft.

- Batch Insert

  100-row records inserted each time, 61,000,000,000 records inserted totally.

   average response time: 54ms, best tps: 800,000

- Select

  - select one record by salt+key+host+ts

    average response time: 5ms

  - select 10-100 records by salt+key+host

    average response time: 50ms(before caching)  *VS*  8ms(after caching) 



# Features

* SQL syntax compatible and restful api supported<br>
  *Users can access by sql client directly, and also by restful api.*

* Dynamic table scheme<br>
  *Table columns is allowed to be added or renamed dynamically.*

* Pre-sharding<br>
  *Pre-sharding is supported when creating a new table.*

* Globally sorted data<br>
  *User can scan tables by primary key globally.*

* Strong consistency.<br>
  *Strong consistency is guaranteed by the data replication works on raft group.*

* Online scalability | Auto failover | Auto rebalance | Auto schedule<br>

* NVMe+SPDK Supported<br>
  *Under testing...*

  


License
-------
Under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.


Thanks
-------
- [Tidb](https://github.com/pingcap/tidb)    

- [cockroachdb](https://github.com/cockroachdb/cockroach)   

- [Kingshard](https://github.com/flike/kingshard)

- [Rocksdb](https://github.com/facebook/rocksdb)

Many designs are inspired by them. Thanks for their great work. 
