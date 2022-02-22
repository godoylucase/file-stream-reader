package sread

import (
	"context"
	"fmt"
	"sync"

	"github.com/godoylucase/s3-file-stream-reader/fstream"
)

type rdr struct {
	conf *WithConfig
}

type WithConfig struct {
	ReadersQty   uint
	OnReadFnName string
}

type Data struct {
	// TODO include chunk metadata here
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

		onReadFn, err := onReadFn(r.conf.OnReadFnName)
		if err != nil {
			e := fmt.Errorf("invalid on read fn name [%v]: %w", r.conf.OnReadFnName, err)
			data <- Data{
				Err: e,
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
