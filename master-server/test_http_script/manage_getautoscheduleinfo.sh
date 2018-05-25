#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
curl -v $TEST_HOST"/manage/getAutoScheduleInfo?d=$ts&s=$sign"


#--------incorrect--------------
