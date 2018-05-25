#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
curl -v $TEST_HOST"/manage/database/getall?d=$ts&s=$sign"
curl -v $TEST_HOST"/manage/database/getall?d=$ts&s=$sign&otherparam=xxx"

