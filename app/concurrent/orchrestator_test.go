package concurrent

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/godoylucase/s3-file-stream-reader/app/concurrent/consumer"
	"github.com/godoylucase/s3-file-stream-reader/app/concurrent/producer"
	"github.com/stretchr/testify/assert"
)

type content struct {
	filename string
	data     int64
}

type strm struct {
	streamFn func(ctx context.Context, fName string, bytesPerRead int64) <-chan producer.BytesStream
}

func (s *strm) Stream(ctx context.Context, fName string, bytesPerRead int64) <-chan producer.BytesStream {
	bs := make(chan producer.BytesStream, 3)

	go func() {
		defer close(bs)
		for i := 1; i <= 100; i++ {
			bs <- producer.BytesStream{
				RangeMetadata: producer.RangeMetadata{
					Filename: "test",
					From:     int64(i - 1),
					To:       int64(i),
				},
				Chunk: []byte(fmt.Sprint(i)),
				Err:   nil,
			}
		}
	}()

	return bs
}

type rdr struct {
	readFn func(ctx context.Context, stream <-chan producer.BytesStream) <-chan consumer.Result
}

func (r *rdr) Read(ctx context.Context, stream <-chan producer.BytesStream) <-chan consumer.Result {
	res := make(chan consumer.Result)

	go func() {
		defer close(res)

		for {
			select {
			case s, ok := <-stream:
				if !ok {
					return
				}

				d, err := strconv.Atoi(string(s.Chunk))
				if err != nil {
					return
				}

				res <- consumer.Result{
					Data: content{
						filename: s.RangeMetadata.Filename,
						data:     int64(d),
					},
					Err: nil,
				}
			default:
			}
		}
	}()

	return res
}

func Test_orch_Run(t *testing.T) {
	orch := New(&strm{}, &rdr{})

	results := orch.Run(context.TODO())
	assert.NotNil(t, results)

	for res := range results {
		assert.NotNil(t, res)
		assert.Nil(t, res.Err)
		assert.NotNil(t, res.Data)

		content, ok := res.Data.(content)
		if !ok {
			t.Fail()
		}

		assert.Equal(t, "test", content.filename)
		assert.Greater(t, content.data, int64(0))
		assert.Less(t, content.data, int64(101))
	}

	_, ok := <-results
	assert.False(t, ok)
}
