package sread

import (
	"context"
	"fmt"
	"sync"

	"github.com/godoylucase/s3-file-stream-reader/internal/fstream"
)

var registry = sync.Map{}

type OnStreamRead func([]byte) (interface{}, error)

func RegisterFn(name string, fn func([]byte) (interface{}, error)) {
	fmt.Printf("registering on read function with name: %+v\n", name)
	registry.Store(name, fn)
}

func OnReadFn(name string) (OnStreamRead, error) {
	load, ok := registry.Load(name)
	fmt.Println(load)
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
	conf *WithConfig
}

type WithConfig struct {
	ReadersQty   uint
	OnReadFnName string
}

type Data struct {
	Content interface{}
	Err     error
}

func New(conf *WithConfig) *rdr {
	return &rdr{
		conf: conf,
	}
}

func (r *rdr) Process(ctx context.Context, stream <-chan fstream.Chunk) <-chan Data {
	data := make(chan Data, r.conf.ReadersQty)

	go func() {
		defer close(data)

		onReadFn, err := OnReadFn(r.conf.OnReadFnName)
		if err != nil {
			data <- Data{
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
							data <- Data{
								Err: s.Err,
							}
							return
						}

						result, err := onReadFn(s.Bytes)
						if err != nil {
							data <- Data{
								Err: err,
							}
							return
						}

						data <- Data{Content: result}
					case <-ctx.Done():
						fmt.Printf("cancelled context: %v\n", ctx.Err())
						data <- Data{Err: ctx.Err()}
						return
					}
				}
			}(&wg)
		}
		wg.Wait()
	}()

	return data
}
