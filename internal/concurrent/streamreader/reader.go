package streamreader

import (
	"context"
	"fmt"
	"sync"

	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/stream"
)

type ParseFn func([]byte) (interface{}, error)

type rdr struct {
	ParseFn
	qty uint
}

type Data struct {
	Content interface{}
	Err     error
}

func New(pf ParseFn, qty uint) *rdr {
	return &rdr{
		ParseFn: pf,
		qty:     qty,
	}
}

func (r *rdr) Process(ctx context.Context, stream <-chan stream.RangeBytes) <-chan Data {
	results := make(chan Data, r.qty)

	go func() {
		defer close(results)

		var wg sync.WaitGroup
		for i := 0; i < int(r.qty); i++ {
			wg.Add(1)

			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					select {
					case s, ok := <-stream:
						if !ok {
							return
						}

						if s.Err != nil {
							results <- Data{
								Err: s.Err,
							}
							return
						}

						parsed, err := r.ParseFn(s.Bytes)
						if err != nil {
							results <- Data{
								Err: err,
							}
							return
						}

						results <- Data{Content: parsed}
					case <-ctx.Done():
						fmt.Printf("cancelled context: %v\n", ctx.Err())
						results <- Data{Err: ctx.Err()}
						return
					}
				}
			}(&wg)
		}
		wg.Wait()
	}()

	return results
}
