1. 参照 INSTALL.md 配置依赖lib，及部署目录<br>
2. cp conf/ds.conf build/ds.conf <br>
3. 修改 ds.conf 内容<br>
    a. 目录：base_path、db_path、log_path和日志级别log_level<br>
    b. master_host配置master集群成员列表,一行一个成员配置<br>
    ``` 
    master_host = 127.0.0.1:7080    
    master_host = 127.0.0.2:7080            
    master_host = 127.0.0.3:7080          
    master_num = 3         
    ```      
   c. check_size 为range的检查size.   
       check_size、split_size、max_size 根据实际情况配置
4. run          
    one. 打包， 参见INSTALL.md.  
    two. 启动     
    ```
        cd build
        ./data-server ./ds.conf start
    ```         
    debug 模式：      
    ```    
        nohup ./data-server ./ds.conf start debug &
    ``` 

## 关键配置项
以下为ds.conf关键的配置信息，需要按照实际情况修改<br>      

```
#应用程序的根目录
base_path =  /home/sharkstore

[rocksdb]

# rocksdb path
path = /home/sharkstore/datas/db

[heartbeat]

# master's ip_addr and port
# may be multiple different master
master_host = 127.0.0.1:7080

# the number of the above master_host
master_num = 1

[log]

#if log path is not set then use base_path
#log path = $log_path + /logs
log_path= /home/sharkstore/logs

#standard log level as syslog, case insensitive, value list:
log_level=info

[worker]

# listen port of recv data
port = 6180

# socket accept thread number
# default value is 1
accept_threads = 1

# epoll recv event thread number
# no default value and must be configured
event_recv_threads = 4

# epoll send event thread number
# no default value and must be configured
event_send_threads = 2

# thread only handle fast tasks. eg. RawGet
fast_worker = 4

# thread only handle slow tasks. eg. select
slow_worker = 8


[range]

# the range real_size is calculated
# if statis_size is greater than check_size
# default value is 32MB
check_size = 512MB

# range split threshold
# default value is 64MB
split_size = 1024MB

# default value is 128MB
max_size = 2048MB


[raft]

# ports used by the raft protocol
port = 18887

# raft log path
log_path = /home/sharkstore/datas/raft


[metric]

# metric report ip
ip_addr = 10.12.142.23

# metric report port
port = 8887

# which cluster to belong to
cluster_id = 1;

```
