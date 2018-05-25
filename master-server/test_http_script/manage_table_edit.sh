#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
curl -v -d "d=$ts&s=$sign&dbname=mydb&tablename=mytb&properties={\"columns\":[{\"data_type\":"7",\"name\":\"myname\"}]}" $TEST_HOST"/manage/table/edit"


#--------incorrect--------------
#curl -v $TEST_HOST"/manage/table/edit?d=$ts&s=$sign&dbname=mydb&tablename=mytb"
#curl -v -d "d=$ts&s=$sign&dbname=mydb&tablename=mytb&properties=aa" $TEST_HOST"/manage/table/create"
#curl -v -d "d=$ts&s=$sign&dbname=mydb&tablename=mytb&properties={\"columns\":[{\"data_type\":"1",\"name\":\"id\",\"primary_key\":"1"},{\"data_type\":"7",\"name\":\"name\",\"primary_key\":\"0\"}]}" $TEST_HOST"/manage/table/edit"
