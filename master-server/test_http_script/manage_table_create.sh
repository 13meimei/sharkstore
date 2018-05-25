#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
curl -v -d "d=$ts&s=$sign&dbname=mydb&tablename=mytb&pkdupcheck=false&properties={\"columns\":[{\"data_type\":"1",\"name\":\"id\",\"primary_key\":"1"}]}" $TEST_HOST"/manage/table/create"


#--------incorrect--------------
#curl -v $TEST_HOST"/manage/table/create?d=$ts&s=$sign&dbname=mydb&tablename=mytb"
#curl -v $TEST_HOST"/manage/table/create?d=$ts&s=$sign&dbname=mydb&tablename=mytb&pkdupcheck=false"
#curl -v $TEST_HOST"/manage/table/create?d=$ts&s=$sign&dbname=mydb&tablename=mytb&properties=aa"
#curl -v -d "d=$ts&s=$sign&dbname=mydb&tablename=mytb&pkdupcheck=false&properties=aa" $TEST_HOST"/manage/table/create"
