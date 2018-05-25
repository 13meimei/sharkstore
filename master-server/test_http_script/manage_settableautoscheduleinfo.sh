#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
curl -v $TEST_HOST"/manage/setTableAutoScheduleInfo?d=$ts&s=$sign&dbName=mydb&tableName=mytb&autoTransferUnable=1&autoFailoverUnable=0"


#--------incorrect--------------
#curl -v $TEST_HOST"/manage/settableAutoScheduleInfo?d=$ts&s=$sign&dbname=mydb&tablename=mytb"
