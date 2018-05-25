#!/bin/sh

nohup ./master-server -config config.toml > nohup.out 2>&1 &

echo "master-server started. pid:[$!]"

echo $! > master-server.pid
