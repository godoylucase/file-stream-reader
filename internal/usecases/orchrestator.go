package usecases

import (
	"context"

	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/stream"
	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/streamreader"
)

type streamer interface {
	Start(ctx context.Context, fName string, bytesPerRead int64) <-chan stream.RangeBytes
}

type reader interface {
	Process(ctx context.Context, stream <-chan stream.RangeBytes) <-chan streamreader.Data
}
type orch struct {
	stream streamer
	reader reader
}

func New(producer streamer, consumer reader) *orch {
	return &orch{
		stream: producer,
		reader: consumer,
	}
}

func (o *orch) Run(ctx context.Context) <-chan streamreader.Data {
	filename := "local-test/example-test-file.txt" // TODO move to somewhere else
	var bpr int64 = 14

	strm := o.stream.Start(ctx, filename, bpr)

	return o.reader.Process(ctx, strm)
}
