#!/bin/bash
protoc -I. raft.proto --cpp_out=.
#protoc -I. raft.proto --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin`
