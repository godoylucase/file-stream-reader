package main

import (
	"github.com/godoylucase/s3-file-stream-reader/cli/cmd"
	"github.com/joho/godotenv"

	_ "github.com/godoylucase/s3-file-stream-reader/example"
)

func main() {
	if err := godotenv.Load(".env"); err != nil {
		panic(err)
	}

	cmd.Execute()
}
