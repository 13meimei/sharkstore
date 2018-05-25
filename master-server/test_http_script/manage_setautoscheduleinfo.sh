#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
curl -v $TEST_HOST"/manage/setAutoScheduleInfo?d=$ts&s=$sign&autoTransferUnable=1&autoFailoverUnable=1"


#--------incorrect--------------
