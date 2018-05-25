#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
curl -v $TEST_HOST"/manage/node/logout?d=$ts&s=$sign&id=1"


#--------incorrect--------------
#curl -v $TEST_HOST"/manage/node/logout?d=$ts&s=$sign"
#curl -v $TEST_HOST"/manage/node/logout?d=$ts&s=$sign&id=aaaa"
#curl -v $TEST_HOST"/manage/node/logout?d=$ts&s=$sign&id=1111111"
