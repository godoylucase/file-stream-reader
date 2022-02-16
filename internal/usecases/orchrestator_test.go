package usecases

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/stream"
	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/streamreader"
	"github.com/stretchr/testify/assert"
)

type content struct {
	filename string
	data     int64
}

type strm struct {
	streamFn func(ctx context.Context, fName string, bytesPerRead int64) <-chan stream.RangeBytes
}

func (s *strm) Start(ctx context.Context, fName string, bytesPerRead int64) <-chan stream.RangeBytes {
	rb := make(chan stream.RangeBytes, 3)

	go func() {
		defer close(rb)
		for i := 1; i <= 100; i++ {
			rb <- stream.RangeBytes{
				Metadata: stream.Metadata{
					Filename: "test",
					From:     int64(i - 1),
					To:       int64(i),
				},
				Bytes: []byte(fmt.Sprint(i)),
				Err:   nil,
			}
		}
	}()

	return rb
}

type rdr struct {
	readFn func(ctx context.Context, stream <-chan stream.RangeBytes) <-chan streamreader.Data
}

func (r *rdr) Process(ctx context.Context, stream <-chan stream.RangeBytes) <-chan streamreader.Data {
	data := make(chan streamreader.Data)

	go func() {
		defer close(data)

		for {
			select {
			case s, ok := <-stream:
				if !ok {
					return
				}

				d, err := strconv.Atoi(string(s.Bytes))
				if err != nil {
					return
				}

				data <- streamreader.Data{
					Content: content{
						filename: s.Metadata.Filename,
						data:     int64(d),
					},
					Err: nil,
				}
			default:
			}
		}
	}()

	return data
}

func Test_orch_Run(t *testing.T) {
	orch := New(&strm{}, &rdr{})

	data := orch.Run(context.TODO())
	assert.NotNil(t, data)

	for d := range data {
		assert.NotNil(t, d)
		assert.Nil(t, d.Err)
		assert.NotNil(t, d.Content)

		content, ok := d.Content.(content)
		if !ok {
			t.Fail()
		}

		assert.Equal(t, "test", content.filename)
		assert.Greater(t, content.data, int64(0))
		assert.Less(t, content.data, int64(101))
	}

	_, ok := <-data
	assert.False(t, ok)
}
