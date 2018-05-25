#!/bin/bash
cp -r ../../../model/proto .
sed -i '/gogo/d' `grep -rl gogo proto`

protoc -I./proto --cpp_out=./gen ./proto/*.proto
protoc -I./proto --grpc_out=./gen --plugin=protoc-gen-grpc=/usr/local/bin/grpc_cpp_plugin ./proto/mspb.proto

rm -rf proto
