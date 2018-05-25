1. cp conf/app.conf conf/app.conf_my (跟cmd/start.sh中的配置对应)
2. 修改 app.conf_my 内容
    a. master的数据目录：datapath和日志目录logpath、日志级别loglevel
    b. cluster.members 配置集群成员，
     [{"id":1,"ip":"127.0.165.52", "http_port":8887,"rpc_port":18887, "raft_ports":[8877,8867]},{"id":1,"ip":"127.0.165.53", "http_port":8887,"rpc_port":18887, "raft_ports":[8877,8867]},{"id":1,"ip":"127.0.165.54", "http_port":8887,"rpc_port":18887, "raft_ports":[8877,8867]}]
3. master的role配置
    a. master | metric 或者 仅metric
         metric.address配置为本服务负责接收处理metric的IP:端口
         metric.url为写监控的mysql【可以是fbase ms-server的http端口】或者为elasticsearch
         metric.type为mysql 或者 elasticsearch, 存储在fbase的类型也为mysql
    b. 仅master
        metric配置忽略
4. run
    one. 打包
        sh build.sh
    two. 启动 [停止]
        sh start.sh [sh stop.sh]

