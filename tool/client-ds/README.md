![hey](http://i.imgur.com/szzD9q0.png)

[![Build Status](https://travis-ci.org/rakyll/hey.svg?branch=master)](https://travis-ci.org/rakyll/hey)

hey is a tiny program that sends some load to a web application.

hey was originally called boom and was influenced from Tarek Ziade's
tool at [tarekziade/boom](https://github.com/tarekziade/boom). Using the same name was a mistake as it resulted in cases
where binary name conflicts created confusion.
To preserve the name for its original owner, we renamed this project to hey.

## Installation

    go get -u github.com/rakyll/hey


Note: Requires go 1.7 or greater.

one:
1. start ds_server
go run simp_ds.go 192.168.61.136:11000 4

2. start ds_client
go run hey.go  -h 192.168.61.136:11000 -n 100000 -c 100  -cpus 4 -m 0


two:
1. 启动一个完整的ds服务 192.168.61.136:6180
2. go run hey.go  -h 192.168.61.136:6180 -n 100000 -c 100  -cpus 4