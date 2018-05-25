#!/bin/sh

if [ ! -e console.pid ]; then
    echo "ERROR: console.pid file not exists!!!" 
    exit 1
fi

kill -9 `cat console.pid`

echo "sharkstore-console had stopped."
