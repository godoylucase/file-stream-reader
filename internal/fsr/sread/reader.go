package sread

import (
	"context"
	"fmt"
	"sync"

	"github.com/godoylucase/file-stream-reader/internal/fsr/fstream"
)

type rdr struct {
	conf *WithConfig
}

type WithConfig struct {
	ReadersQty   uint
	OnReadFnName string
}

type Metadata struct {
	Filename string
	FromByte int64
	ToByte   int64
}

func (m *Metadata) String() string {
	return fmt.Sprintf("{Filename: %v FromByte: %v ToByte: %v}", m.Filename, m.FromByte, m.ToByte)
}

type Data struct {
	Content  interface{}
	Metadata *Metadata
	Err      error
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

						meta := &Metadata{
							Filename: s.Filename,
							FromByte: s.From,
							ToByte:   s.To,
						}

						if s.Err != nil {
							data <- Data{
								Err:      s.Err,
								Metadata: meta,
							}
							return
						}

						result, err := onReadFn(s.Bytes)
						if err != nil {
							data <- Data{
								Err:      err,
								Metadata: meta,
							}
							return
						}

						data <- Data{
							Content:  result,
							Metadata: meta,
						}
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
