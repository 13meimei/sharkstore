# SHARKSTORE

[English Version](README_EN.md)

sharkstore是一个分布式的持久化K-V存储系统，存储层依赖rocksdb，数据副本之间通过raft协议进行复制<br>

系统主要包括<br>
元数据管理模块：master-server(golang)，<br>
业务数据存储模块：data-server(c++)，<br>
访问代理层：gateway-server(golang)，<br>
管理端：console(golang)<br><br>

目录结构：<br>

```
|-- README.md
|-- console             web管理端
|-- data-server         业务数据存储服务
|-- glide.yaml
|-- master-server       元数据管理服务
|-- model
|-- pkg-go              公共模块
|-- proxy
    |-- gateway-server  sql/http rest代理层
```

# 架构设计
详细查看[arch.md](doc/arch.md)<br>

# 安装说明
详细查看[INSTALL.md](INSTALL.md)<br>

# 测试数据：
压测表名:metric,3台dataserver物理机【NVMe盘】，<br>
压测表metric有12个column，每个column都为int类型，<br>
前4个column(salt,key,host,ts)组成联合索引做为key，<br>
对metric表预分裂100个range，三个副本，raft复制<br>

### 批量插入：

 一次批量插入100行记录：测的最大TPS为80W笔/秒，平均响应时间为56ms，累计插入了610亿条数据<br>
 
### 查询负载测试：
1) 根据salt+key+host+ts查询单条数据：平均响应时间为5ms<br>

2) 根据salt+key+host进行查询(10-100条记录)：平均响应时间为50ms，所查数据有了缓存之后，平均响应时间8ms。<br>




# 特性
--------
* 兼容SQL语法、支持Restful API<br>
	用户可以直接使用 SQL 客户端进行访问，或使用 Restful API 的方式进行读写。

* 动态表结构<br>
	一个表的列可以动态添加或重命名

* 预分片<br>
	创建表时可以进行预分片

* 全局数据有序<br>
	用户可以使用主键进行扫描

* 强一致<br>
	数据通过 raft 组复制，保证强一致性

* 在线伸缩/自动故障恢复/自动平衡/自动调度<br>

* NVMe+SPDK<br>
	测试中...


License
-------
Under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.
