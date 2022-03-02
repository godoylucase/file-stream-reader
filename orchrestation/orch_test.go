package orchrestation

import (
	"context"
	"strconv"

	"github.com/godoylucase/file-stream-reader/internal/fsr/fstream"
	"github.com/godoylucase/file-stream-reader/internal/fsr/sread"
)

type content struct {
	filename string
	data     int64
}

type strm struct {
	streamFn func(ctx context.Context, fName string, bytesPerRead int64) <-chan fstream.Chunk
}

type rdr struct {
	readFn func(ctx context.Context, stream <-chan fstream.Chunk) <-chan sread.Data
}

func (r *rdr) Process(ctx context.Context, stream <-chan fstream.Chunk) <-chan sread.Data {
	data := make(chan sread.Data)

	go func() {
		defer close(data)

		for {
			select {
			case s, ok := <-stream:
				if !ok {
					return
				}

				d, err := strconv.Atoi(string(s.Bytes))
				if err != nil {
					return
				}

				data <- sread.Data{
					Content: content{
						filename: s.Filename,
						data:     int64(d),
					},
					Err: nil,
				}
			default:
			}
		}
	}()

	return data
}
