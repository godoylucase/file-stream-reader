package consumer

import (
	"context"
	"github.com/godoylucase/s3-file-stream-reader/producer"
	"github.com/stretchr/testify/assert"
	"testing"
)

var expected = []string{
	"00-00", "10-01", "20-02", "30-03", "40-04",
	"50-05", "60-06", "70-07", "80-08", "90-09",
}

type res struct {
	typ   string
	value string
	raw   string
}

func TestStreamReader_Read(t *testing.T) {
	expMap := make(map[string]struct{}, len(expected))

	reader := New(func(bytes []byte) (interface{}, error) {
		typ := bytes[:2]
		val := bytes[3:]

		return res{
			string(typ),
			string(val),
			string(bytes),
		}, nil
	})

	streams := stream(expected)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reads := reader.Read(ctx, streams)
	i := 0

readChannel:
	for {
		select {
		case r, ok := <-reads:
			if !ok {
				break readChannel
			}

			if r.Err != nil {
				break readChannel
			}

			res, ok := r.Data.(res)
			if !ok {
				break readChannel
			}

			expMap[res.raw] = struct{}{}

			i++
		case <-ctx.Done():
			return
		default:
		}

	}

	assert.Len(t, expMap, 10)
	for _, v := range expected {
		exp, ok := expMap[v]
		if !ok {
			t.Fail()
		}
		assert.NotNil(t, exp)
	}
}

func stream(values []string) <-chan producer.BytesStream {
	out := make(chan producer.BytesStream, workerCount)

	go func() {
		defer close(out)
		for _, e := range values {
			out <- producer.BytesStream{
				RangeMetadata: producer.RangeMetadata{},
				Chunk:         []byte(e),
			}
		}
	}()

	return out
}
