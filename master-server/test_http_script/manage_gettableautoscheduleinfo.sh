#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
curl -v $TEST_HOST"/manage/getTableAutoScheduleInfo?d=$ts&s=$sign&dbName=mydb&tableName=mytb"


#--------incorrect--------------
#curl -v $TEST_HOST"/manage/gettableAutoScheduleInfo?d=$ts&s=$sign&dbname=mydb&tablename=mytb"
