package fstream

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
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
	byteRangeFn     func(filename string, from int64, chunk []byte) error
}

func (s *storageMock) Length(filename string) (int64, error) {
	return s.contentLengthFn(filename)
}

func (s *storageMock) GetBytes(filename string, from int64, chunk []byte) error {
	return s.byteRangeFn(filename, from, chunk)
}

func Test_producer_Stream(t *testing.T) {
	src := &storageMock{
		contentLengthFn: func(filename string) (int64, error) {
			assert.Equal(t, fName, filename)
			return int64(len(even)), nil
		},
		byteRangeFn: func(filename string, from int64, chunk []byte) error {
			assert.Equal(t, fName, filename)
			assert.NotEmpty(t, chunk)

			_ = copy(chunk, even[from:from+int64(len(chunk))])
			return nil
		},
	}
	p := &strm{
		fsource: src,
		conf: &WithConfig{
			Streamers: 1,
			ChunkSize: 3,
			Filename:  fName,
		},
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer func() { cancelFunc() }()

	stream := p.Start(ctx)
	assert.NotNil(t, stream)

	for s := range stream {
		assert.Equal(t, fName, s.Filename)
		assert.Nil(t, s.Err)
		assert.NotEmpty(t, s.Bytes)
		assert.Equal(t, "22", string(s.Bytes)[:2])
	}

}

func Test_streamOddRanges(t *testing.T) {
	count := 0
	for value := range chunkRanges("any", int64(len(odd)), bpr) {
		if count == 2 {
			assert.True(t, value.from+1 == value.to)
		}
		count++
	}

	assert.Equal(t, 3, count)
}

func Test_streamEvenRanges(t *testing.T) {
	count := 0
	for value := range chunkRanges("any", int64(len(even)), bpr) {
		fmt.Printf("+%v\n", value)
		count++
	}

	assert.Equal(t, 3, count)
}
