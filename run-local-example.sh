#!/bin/bash

chunkSize=14

# runs the streaming process from a local file
go run ./cli/main.go streamFile \
  type=local-file \
  reader-fn=example \
  path=./example/example-test-file.txt \
  chunk-size=$chunkSize \
  streamers=10 \
  readers=10
