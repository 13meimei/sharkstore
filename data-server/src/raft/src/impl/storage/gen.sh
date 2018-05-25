#!/bin/bash
protoc -I. log_format.proto --cpp_out=. 
