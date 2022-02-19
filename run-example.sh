#!/bin/bash

bucket=local-test
key=/example-test-file.txt
chunkSize=14

# prepares test file
go run ./cli/main.go pushToS3 \
  from=./mock/example-test-file.txt \
  bucket=$bucket \
  key=$key

# runs the streaming process
go run ./cli/main.go streamFile \
  type=s3-bucket \
  reader-fn=example \
  bucket=$bucket \
  key=$key \
  chunk-size=$chunkSize \
  streamers=10 \
  readers=10
