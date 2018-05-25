#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
curl -v $TEST_HOST"/manage/table/getall?d=$ts&s=$sign&dbname=mydb"

#--------incorrect--------------
#curl -v $TEST_HOST"/manage/table/getall?d=$ts&s=$sign"
