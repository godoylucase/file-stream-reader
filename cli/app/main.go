package main

import (
	"context"
	"fmt"

	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/stream"
	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/streamreader"
	"github.com/godoylucase/s3-file-stream-reader/internal/platform/awss3"
	"github.com/godoylucase/s3-file-stream-reader/internal/usecases"
	"github.com/godoylucase/s3-file-stream-reader/internal/usecases/example"
	_ "github.com/joho/godotenv/autoload"
)

func main() {
	s3, err := awss3.NewProxy()
	if err != nil {
		panic(err)
	}

	s := stream.New(s3, 1)
	sr := streamreader.New(example.ParseFn(), 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		fmt.Println("canceled context")
		fmt.Println("exiting stream reader")
		cancel()
	}()

	orch := usecases.New(s, sr)
	for d := range orch.Run(ctx) {
		fmt.Printf("result %+v\n", d)
	}
}
