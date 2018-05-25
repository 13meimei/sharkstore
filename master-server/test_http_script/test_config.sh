#!/bin/sh

TEST_HOST="http://127.0.0.1:8887"

TEST_CLUSTERID=1
TEST_SIGN_TOKEN="test"


ts=
sign=
calc_sign() {
    ts=`date '+%s'`
    md5str=$TEST_CLUSTERID$ts$TEST_SIGN_TOKEN
    #echo "md5str="$md5str

    sign=`echo -n $md5str | md5sum | awk '{print $1}'`
    #echo "sing="$sign
}

