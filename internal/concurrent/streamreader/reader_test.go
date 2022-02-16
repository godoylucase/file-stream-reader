package streamreader

import (
	"context"
	"testing"

	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/stream"
	"github.com/stretchr/testify/assert"
)

var expected = []string{
	"00-00", "10-01", "20-02", "30-03", "40-04",
	"50-05", "60-06", "70-07", "80-08", "90-09",
}

type dat struct {
	typ   string
	value string
	raw   string
}

func TestStreamReader_Read(t *testing.T) {
	expMap := make(map[string]struct{}, len(expected))

	reader := New(func(bytes []byte) (interface{}, error) {
		typ := bytes[:2]
		val := bytes[3:]

		return dat{
			string(typ),
			string(val),
			string(bytes),
		}, nil
	}, 1)

	strm := produce(expected)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sdata := reader.Process(ctx, strm)
	i := 0

readChannel:
	for {
		select {
		case sd, ok := <-sdata:
			if !ok {
				break readChannel
			}

			if sd.Err != nil {
				break readChannel
			}

			res, ok := sd.Content.(dat)
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

func produce(values []string) <-chan stream.RangeBytes {
	out := make(chan stream.RangeBytes, 2)

	go func() {
		defer close(out)
		for _, e := range values {
			out <- stream.RangeBytes{
				Metadata: stream.Metadata{},
				Bytes:    []byte(e),
			}
		}
	}()

	return out
}
