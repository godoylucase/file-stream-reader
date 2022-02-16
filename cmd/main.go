package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/stream"
	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/streamreader"
	"github.com/godoylucase/s3-file-stream-reader/internal/platform"
	"github.com/godoylucase/s3-file-stream-reader/internal/usecases"
	"github.com/godoylucase/s3-file-stream-reader/internal/usecases/example"
	_ "github.com/joho/godotenv/autoload"
)

func main() {
	s3, err := platform.NewS3Proxy()
	if err != nil {
		panic(err)
	}

	s := stream.New(s3, 1)
	sr := streamreader.New(example.ParseFn(), 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		fmt.Println("canceled context")
		cancel()
	}()

	orch := usecases.New(s, sr)
	for res := range orch.Run(ctx) {
		fmt.Printf("result %+v\n", res)
	}

	sigquit := make(chan os.Signal, 1)
	signal.Notify(sigquit, os.Interrupt, syscall.SIGTERM)
	sig := <-sigquit
	log.Printf("caught sig: %+v\n", sig)
	log.Printf("Gracefully shutting down...\n")

}
