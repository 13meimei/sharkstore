## Example
启动三个节点：
./playground -node 1 -peers 1,2,3 -dir ./data1
./playground -node 2 -peers 1,2,3 -dir ./data2
./playground -node 3 -peers 1,2,3 -dir ./data3

## 监听地址
监听地址内定，不用外部指定：                   
heartbeat: 99{nodeID}1     
replicate: 99{nodeID}2     
client:    99{nodeID}3     
因此最多支持同时启动九个节点    
需要确保相应端口没有被占用      

## 客户端连接
客户端可以通过telnet连接节点的99{nodeID}3端口发送命令，     
支持的命令列表可通过help命令获取          

状态机为一个sum变量，raft命令为一个数字 num，每次命令执行 sum 增加 num
