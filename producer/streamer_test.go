package producer

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	odd = `11
11
1`

	even = `22
22
22`

	bpr   int64 = 3
	fName       = "some/filename"
)

type storageMock struct {
	contentLengthFn func(filename string) (int64, error)
	byteRangeFn     func(filename string, from, to int64, chunk []byte) error
}

func (s *storageMock) ContentLength(filename string) (int64, error) {
	return s.contentLengthFn(filename)
}

func (s *storageMock) ByteRange(filename string, from, to int64, chunk []byte) error {
	return s.byteRangeFn(filename, from, to, chunk)
}

func Test_producer_Stream(t *testing.T) {
	p := &producer{
		s: &storageMock{
			contentLengthFn: func(filename string) (int64, error) {
				assert.Equal(t, fName, filename)
				return int64(len(even)), nil
			},
			byteRangeFn: func(filename string, from, to int64, chunk []byte) error {
				assert.Equal(t, fName, filename)
				assert.NotEmpty(t, chunk)

				_ = copy(chunk, even[from:to])
				return nil
			},
		},
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer func() { cancelFunc() }()

	stream := p.Stream(ctx, fName, bpr)
	assert.NotNil(t, stream)

	for s := range stream {
		assert.Equal(t, fName, s.Filename)
		assert.Nil(t, s.Err)
		assert.NotEmpty(t, s.Chunk)
		assert.Equal(t, "22", string(s.Chunk)[:2])
	}

}

func Test_streamOddRanges(t *testing.T) {
	count := 0
	for value := range streamRanges("any", int64(len(odd)), bpr) {
		if count == 2 {
			assert.True(t, value.From+1 == value.To)
		}
		count++
	}

	assert.Equal(t, 3, count)
}

func Test_streamEvenRanges(t *testing.T) {
	count := 0
	for value := range streamRanges("any", int64(len(even)), bpr) {
		fmt.Printf("+%v\n", value)
		count++
	}

	assert.Equal(t, 3, count)
}
