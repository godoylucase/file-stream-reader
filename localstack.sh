#!/bin/bash

endpoint=http://localhost:4566
bucket=local-test
testFile=./mock/test-file.txt

echo "creating bucket '$bucket' over '$endpoint'"
aws --endpoint-url=$endpoint s3 mb s3://$bucket

echo "moving $testFile to bucket $bucket"
aws --endpoint-url=$endpoint s3 cp $testFile s3://$bucket