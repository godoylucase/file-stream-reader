/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package main

import (
	"github.com/godoylucase/s3-file-stream-reader/cli/cmd"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(".env"); err != nil {
		panic(err)
	}

	cmd.Execute()
}
