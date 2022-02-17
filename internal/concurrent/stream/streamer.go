package stream

import (
	"context"
	"fmt"
	"sync"
)

const (
	rangeBufferSize = 10
)

type source interface {
	ContentLength(filename string) (int64, error)
	ByteRange(filename string, from, to int64, chunk []byte) error
}

type Metadata struct {
	Filename string
	From     int64
	To       int64
}

type RangeBytes struct {
	Metadata
	Bytes []byte
	Err   error
}

type strm struct {
	src source
	qty uint
}

func New(src source, qty uint) *strm {
	return &strm{
		src: src,
		qty: qty,
	}
}

func (s *strm) Start(ctx context.Context, fName string, bytesPerRead int64) <-chan RangeBytes {
	// TODO move arguments as config struct properties

	stream := make(chan RangeBytes, s.qty)

	length, err := s.src.ContentLength(fName)
	if err != nil {
		defer close(stream)
		stream <- RangeBytes{
			Bytes: nil,
			Err:   err,
		}
		return stream
	}

	ranges := ranges(fName, length, bytesPerRead)

	go func() {
		defer close(stream)

		var wg sync.WaitGroup
		for i := 0; i < int(s.qty); i++ {
			wg.Add(1)

			// for each worker run a goroutine To read From the ranges channel and reach
			// such range From the src
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					select {
					case r, ok := <-ranges:
						if !ok {
							return
						}
						chunk := make([]byte, bytesPerRead)
						if err := s.src.ByteRange(fName, r.From, r.To, chunk); err != nil {
							stream <- RangeBytes{
								Metadata: r,
								Err:      err,
							}
							return
						}

						stream <- RangeBytes{
							Metadata: r,
							Bytes:    chunk,
						}
					case <-ctx.Done():
						fmt.Printf("cancelled context: %v\n", ctx.Err())
						stream <- RangeBytes{
							Err: ctx.Err(),
						}
						return
					}
				}
			}(&wg)
		}
		wg.Wait()
	}()

	return stream
}

func ranges(filename string, length int64, bpr int64) <-chan Metadata {
	ranges := length / bpr

	// allocates ranges To be processed later on by reading the returned channel
	rangesBuffer := make(chan Metadata, rangeBufferSize)
	go func() {
		defer close(rangesBuffer)

		for i := int64(0); i <= ranges; i++ {
			from := i * bpr
			to := from + bpr
			if to > length {
				to = length
			}

			rm := Metadata{
				Filename: filename,
				From:     from,
				To:       to,
			}

			rangesBuffer <- rm
		}
	}()

	return rangesBuffer
}
