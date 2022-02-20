package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"

	_ "github.com/godoylucase/s3-file-stream-reader/usecase/example"
)

//go run ./cli/main.go streamFile \
//type=s3-bucket \
//reader-fn=example \
//bucket=$bucket \
//key=$key \
//chunk-size=$chunkSize \
//streamers=10 \
//readers=10

func Test_commandFn(t *testing.T) {
	args := []string{
		"type=s3-bucket",
		"reader-fn=example",
		"bucket=local-test",
		"key=/example-test-file.txt",
		"chunk-size=14",
		"streamers=10",
		"readers=10",
	}
	cmd := streamFile()
	cmd.SetArgs(args)

	err := cmd.Execute()

	assert.Nil(t, err)
}
