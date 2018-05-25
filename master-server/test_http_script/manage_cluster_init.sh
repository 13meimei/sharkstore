#!/bin/sh

source ./test_config.sh
calc_sign

curl -v $TEST_HOST"/manage/cluster/init?d=$ts&s=$sign"

#------------incorrect----------
