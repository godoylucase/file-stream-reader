package filestream

import (
	"context"
	"fmt"
	"sync"
)

type Source interface {
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

type Config struct {
	Source       Source
	StreamersQty uint
	BytesPerRead int64
	Filename     string
}

type strm struct {
	conf *Config
}

func New(cfg *Config) *strm {
	return &strm{
		conf: cfg,
	}
}

func (s *strm) Start(ctx context.Context) <-chan RangeBytes {
	filename := s.conf.Filename

	stream := make(chan RangeBytes, s.conf.StreamersQty)

	length, err := s.conf.Source.ContentLength(filename)
	if err != nil {
		defer close(stream)
		stream <- RangeBytes{
			Bytes: nil,
			Err:   err,
		}
		return stream
	}

	ranges := ranges(filename, length, s.conf.BytesPerRead, s.conf.StreamersQty)

	go func() {
		defer close(stream)

		var wg sync.WaitGroup
		for i := 0; i < int(s.conf.StreamersQty); i++ {
			wg.Add(1)

			// for each worker run a goroutine To read From the ranges channel and reach
			// such range From the Source
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					select {
					case r, ok := <-ranges:
						if !ok {
							return
						}
						chunk := make([]byte, s.conf.BytesPerRead)
						if err := s.conf.Source.ByteRange(filename, r.From, r.To, chunk); err != nil {
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

func ranges(filename string, length, bpr int64, qty uint) <-chan Metadata {
	ranges := length / bpr

	// allocates ranges To be processed later on by reading the returned channel
	rangesBuffer := make(chan Metadata, qty)
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
