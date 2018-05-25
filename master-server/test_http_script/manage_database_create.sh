#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
#curl -v $TEST_HOST"/manage/database/create?d=$ts&s=$sign&name=mydb"
curl -v $TEST_HOST"/manage/database/create?d=$ts&s=$sign&name=mydb&properties=mydb_prop"


#--------incorrect--------------
#curl -v $TEST_HOST"/manage/database/create?d=$ts&s=$sign&name=mydb"
#curl -v $TEST_HOST"/manage/database/create?name=mydb"
#curl -v $TEST_HOST"/manage/database/create?d=&name=mydb"
#curl -v $TEST_HOST"/manage/database/create?d=1514358409&s=57687c59342c4b5d46c5fa9e48b37925&name=mydb"

#curl -v $TEST_HOST"/manage/database/create?d=$ts&s=$sign"
