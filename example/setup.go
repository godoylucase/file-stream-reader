package example

import (
	"strconv"
	"strings"
	"time"

	"github.com/godoylucase/s3-file-stream-reader/sread"
)

const (
	fnName = "example"
)

type data struct {
	id     string
	date   *time.Time
	amount string
}

func init() {
	sread.RegisterFn(fnName, fn)
}

func fn(b []byte) (interface{}, error) {
	id := string(b[:2])
	amount := strings.Trim(string(b[11:]), "\n")

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
		amount: amount,
	}

	return parsed, nil
}
