#!/bin/sh

if [ ! -e master-server.pid ]; then
    echo "ERROR: master-server.pid file not exists!!!" 
    exit 1
fi

kill -9 `cat master-server.pid`

echo "master-server had stopped."
