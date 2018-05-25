#!/bin/sh
nohup ./sharkstore-console --config=./console.conf > nohup.out 2>&1 &

echo "sharkstore-console started. pid:[$!]"

echo $! > console.pid
