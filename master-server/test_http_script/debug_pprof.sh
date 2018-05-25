#!/bin/sh

source ./test_config.sh

#------------correct-----------
curl -v $TEST_HOST"/debug/ping"
curl -v $TEST_HOST"/debug/gc"

curl -v $TEST_HOST"/debug/pprof/"
curl -v $TEST_HOST"/debug/pprof/heap?file=heap.prof"
curl -v $TEST_HOST"/debug/pprof/goroutine?file=goroutine.prof"

curl -v $TEST_HOST"/debug/pprof/symbol?file=symbol.prof"
curl -v $TEST_HOST"/debug/pprof/profile?file=profile.prof"
curl -v $TEST_HOST"/debug/pprof/trace?file=trace.prof"
curl -v $TEST_HOST"/debug/pprof/cmdline?file=cmdline.prof"

#------------incorrect----------
#curl -v $TEST_HOST"/debug/pprof/myheap?file=myheap.prof"
