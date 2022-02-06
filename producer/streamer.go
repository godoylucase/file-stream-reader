package producer

import (
	"context"
	"fmt"
	"sync"
)

const (
	rangeBufferSize = 3
	workerCount     = 3
)

type storage interface {
	ContentLength(filename string) (int64, error)
	ByteRange(filename string, from, to int64, chunk []byte) error
}

type RangeMetadata struct {
	Filename string
	From     int64
	To       int64
}

type BytesStream struct {
	RangeMetadata
	Chunk []byte
	Err   error
}

type producer struct {
	s storage
}

func New(s storage) *producer {
	return &producer{s: s}
}

func (p *producer) Stream(ctx context.Context, fName string, bytesPerRead int64) <-chan BytesStream {
	stream := make(chan BytesStream, workerCount)

	length, err := p.s.ContentLength(fName)
	if err != nil {
		stream <- BytesStream{
			Chunk: nil,
			Err:   err,
		}
		return stream
	}

	rangeMeta := streamRanges(fName, length, bytesPerRead)

	go func() {
		defer close(stream)

		var wg sync.WaitGroup
		for i := 0; i < workerCount; i++ {
			wg.Add(1)

			// for each worker run a goroutine To read From the rangeMeta channel and reach
			// such range From the storage
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					select {
					case rm, ok := <-rangeMeta:
						if !ok {
							return
						}
						chunk := make([]byte, bytesPerRead)
						if err := p.s.ByteRange(fName, rm.From, rm.To, chunk); err != nil {
							stream <- BytesStream{
								RangeMetadata: rm,
								Err:           err,
							}
							return
						}

						stream <- BytesStream{
							RangeMetadata: rm,
							Chunk:         chunk,
						}
					case <-ctx.Done():
						fmt.Printf("cancelled context: %v\n", ctx.Err())
						stream <- BytesStream{
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

func streamRanges(filename string, length int64, bpr int64) <-chan RangeMetadata {
	ranges := length / bpr
	remaining := length % bpr

	// allocates ranges To be processed later on by reading the returned channel
	rangesBuffer := make(chan RangeMetadata, rangeBufferSize)
	go func() {
		defer close(rangesBuffer)

		for i := int64(0); i <= ranges; i++ {
			from := i * bpr
			to := from + bpr
			if to > length {
				to = from + remaining
			}

			rm := RangeMetadata{
				Filename: filename,
				From:     from,
				To:       to,
			}

			fmt.Printf("sending buffer metadata %+v To channel\n", rm)

			rangesBuffer <- rm
		}
	}()

	return rangesBuffer
}
