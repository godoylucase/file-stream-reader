package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/godoylucase/s3-file-stream-reader/producer"
)

const (
	workerCount = 3
)

type ParseFn func([]byte) (interface{}, error)

type consumer struct {
	ParseFn
}

type Result struct {
	Data interface{}
	Err  error
}

func New(pf ParseFn) *consumer {
	return &consumer{pf}
}

func (sr *consumer) Read(ctx context.Context, stream <-chan producer.BytesStream) <-chan Result {
	results := make(chan Result, workerCount)

	go func() {
		defer close(results)

		var wg sync.WaitGroup
		for i := 0; i < workerCount; i++ {
			wg.Add(1)

			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					select {
					case s, ok := <-stream:
						if !ok {
							return
						}

						parsed, err := sr.ParseFn(s.Chunk)
						if err != nil {
							results <- Result{
								Err: err,
							}
							return
						}

						results <- Result{Data: parsed}
					case <-ctx.Done():
						fmt.Printf("cancelled context: %v\n", ctx.Err())
						results <- Result{Err: ctx.Err()}
						return
					}
				}
			}(&wg)
		}
		wg.Wait()
	}()

	return results
}
