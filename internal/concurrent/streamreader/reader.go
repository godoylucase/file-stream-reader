package streamreader

import (
	"context"
	"fmt"
	"sync"

	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/filestream"
)

var registry = sync.Map{}

type OnStreamRead func([]byte) (interface{}, error)

func RegisterFn(name string, fn OnStreamRead) {
	registry.Store(name, fn)
}

func OnReadFn(name string) (OnStreamRead, error) {
	load, ok := registry.Load(name)
	if !ok {
		return nil, fmt.Errorf("%v is not a valid on read function", name)
	}

	fn, ok := load.(func([]byte) (interface{}, error))
	if !ok {
		return nil, fmt.Errorf("%v is not a valid on read function", name)
	}

	return fn, nil
}

type rdr struct {
	conf *Config
}

type Config struct {
	ReadersQty   uint
	OnReadFnName string
}

type Data struct {
	Content interface{}
	Err     error
}

func New(cfg *Config) *rdr {
	return &rdr{
		conf: cfg,
	}
}

func (r *rdr) Process(ctx context.Context, stream <-chan filestream.RangeBytes) <-chan Data {
	results := make(chan Data, r.conf.ReadersQty)

	go func() {
		defer close(results)

		onReadFn, err := OnReadFn(r.conf.OnReadFnName)
		if err != nil {
			results <- Data{
				Err: err,
			}
			return
		}

		var wg sync.WaitGroup
		for i := 0; i < int(r.conf.ReadersQty); i++ {
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

						parsed, err := onReadFn(s.Bytes)
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
