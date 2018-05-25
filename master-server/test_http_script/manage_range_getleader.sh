#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
#curl -v $TEST_HOST"/manage/range/getleader?d=$ts&s=$sign&rangeid="


#--------incorrect--------------
