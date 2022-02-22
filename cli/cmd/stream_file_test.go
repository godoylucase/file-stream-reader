package cmd

import (
	"testing"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"

	_ "github.com/godoylucase/s3-file-stream-reader/example"
)

func Test_commandFn(t *testing.T) {
	if err := godotenv.Load("./../../.env"); err != nil {
		t.Fail()
	}

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

func Test_commandFn_localFile(t *testing.T) {
	args := []string{
		"type=local-file",
		"reader-fn=example",
		"path=../../example/example-test-file.txt",
		"chunk-size=14",
		"streamers=10",
		"readers=10",
	}
	cmd := streamFile()
	cmd.SetArgs(args)

	err := cmd.Execute()

	assert.Nil(t, err)
}
