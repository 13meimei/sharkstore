#!/bin/sh

source ./test_config.sh
calc_sign

#--------correct--------------
curl -v -d "d=$ts&s=$sign&dbname=mydb&tablename=mysqltb&sql='create table mysqltb(userPin varchar(50) NOT NULL COMMENT 'user pin', periods varchar(12) NOT NULL COMMENT 'periods', primary key(userPin,periods))' " $TEST_HOST"/manage/sql/table/create"


#--------incorrect--------------
#curl -v -d "d=$ts&s=$sign&dbname=mydb&tablename=mysqltb&sql='CREATE TABLE mysqltb (userPin varchar(50) NOT NULL COMMENT 'user pin', periods varchar(12) NOT NULL COMMENT 'periods', PRIMARY KEY (userPin,periods))' " $TEST_HOST"/manage/sql/table/create"
#curl -v -d "d=$ts&s=$sign&dbname=mydb&tablename=mysqltb&sql='CREATE TABLE `mysqltb` (`userPin` varchar(50) NOT NULL COMMENT 'user pin', periods varchar(12) NOT NULL COMMENT 'periods', PRIMARY KEY (userPin,periods))' " $TEST_HOST"/manage/sql/table/create"
