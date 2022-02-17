#!/bin/bash

# prepares test file
go run ./cli/main.go pushToS3 from=./mock/example-test-file.txt bucket=local-test key=/example-test-file.txt

go run ./cli/app/main.go