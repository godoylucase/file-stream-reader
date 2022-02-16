package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/godoylucase/s3-file-stream-reader/app/concurrent"
	"github.com/godoylucase/s3-file-stream-reader/app/concurrent/consumer"
	"github.com/godoylucase/s3-file-stream-reader/app/concurrent/producer"
	"github.com/godoylucase/s3-file-stream-reader/app/infra"
	_ "github.com/joho/godotenv/autoload"
)

type data struct {
	id     string
	date   *time.Time
	random string
}

func main() {
	s3, err := infra.NewS3Proxy()
	if err != nil {
		panic(err)
	}

	p := producer.New(s3)

	parseFn := func(b []byte) (interface{}, error) {
		id := string(b[:2])
		digits := strings.Trim(string(b[11:]), "\n")

		y := string(b[2:7])
		m := string(b[7:9])
		d := string(b[9:11])

		year, err := strconv.Atoi(y)
		if err != nil {
			return nil, err
		}

		month, err := strconv.Atoi(m)
		if err != nil {
			return nil, err
		}

		day, err := strconv.Atoi(d)
		if err != nil {
			return nil, err
		}

		date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)

		parsed := data{
			id:     id,
			date:   &date,
			random: digits,
		}

		//fmt.Printf("parsed line as %+v\n", parsed)

		return parsed, nil
	}

	c := consumer.New(parseFn)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		fmt.Println("canceled context")
		cancel()
	}()

	orch := concurrent.New(p, c)
	for res := range orch.Run(ctx) {
		fmt.Printf("result %+v\n", res)
	}

	sigquit := make(chan os.Signal, 1)
	signal.Notify(sigquit, os.Interrupt, syscall.SIGTERM)
	sig := <-sigquit
	log.Printf("caught sig: %+v\n", sig)
	log.Printf("Gracefully shutting down...\n")

}
