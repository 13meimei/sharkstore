#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
curl -v $TEST_HOST"/manage/table/delete?d=$ts&s=$sign&dbname=mydb&tablename=mytb"


#--------incorrect--------------
#curl -v $TEST_HOST"/manage/table/delete?d=$ts&s=$sign&dbname=mydb&tablename=m"
