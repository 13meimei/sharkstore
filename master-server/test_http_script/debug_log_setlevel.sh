#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
curl -v $TEST_HOST"/debug/log/setlevel?d=$ts&s=$sign&level=info"


#--------incorrect--------------
