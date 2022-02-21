package fstream

import (
	"context"
	"sync"
)

type FileSource interface {
	Length(filename string) (int64, error)
	Bytes(filename string, from, to int64, chunk []byte) error
}

type metadata struct {
	filename string
	from     int64
	to       int64
}

type Chunk struct {
	Filename string
	From     int64
	To       int64
	Bytes    []byte
	Err      error
}

type WithConfig struct {
	Streamers uint
	ChunkSize int64
	Filename  string
}

type strm struct {
	fsource FileSource
	conf    *WithConfig
}

func New(fs FileSource, conf *WithConfig) *strm {
	return &strm{
		fsource: fs,
		conf:    conf,
	}
}

func (s *strm) Start(ctx context.Context) <-chan Chunk {
	filename := s.conf.Filename

	stream := make(chan Chunk, s.conf.Streamers)

	length, err := s.fsource.Length(filename)
	if err != nil {
		defer close(stream)
		stream <- Chunk{
			Filename: filename,
			Err:      err,
		}
		return stream
	}

	ranges := chunkRanges(filename, length, s.conf.ChunkSize)

	go func() {
		defer close(stream)

		var wg sync.WaitGroup
		for i := 0; i < int(s.conf.Streamers); i++ {
			wg.Add(1)

			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					select {
					case r, ok := <-ranges:
						if !ok {
							return
						}

						chunk := make([]byte, s.conf.ChunkSize)
						if err := s.fsource.Bytes(filename, r.from, r.to, chunk); err != nil {
							stream <- Chunk{
								Filename: r.filename,
								From:     r.from,
								To:       r.to,
								Err:      err,
							}
							return
						}

						stream <- Chunk{
							Filename: r.filename,
							From:     r.from,
							To:       r.to,
							Bytes:    chunk,
						}
					case <-ctx.Done():
						stream <- Chunk{
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

func chunkRanges(filename string, length, chunkSize int64) <-chan metadata {
	qty := length / chunkSize

	buf := make(chan metadata, qty)
	go func() {
		defer close(buf)

		for i := int64(0); i <= qty; i++ {
			from := i * chunkSize
			to := from + chunkSize
			if to > length {
				to = length
			}

			meta := metadata{
				filename: filename,
				from:     from,
				to:       to,
			}

			buf <- meta
		}
	}()

	return buf
}
