package concurrent

import (
	"context"

	"github.com/godoylucase/s3-file-stream-reader/app/concurrent/consumer"
	"github.com/godoylucase/s3-file-stream-reader/app/concurrent/producer"
)

type streamer interface {
	Stream(ctx context.Context, fName string, bytesPerRead int64) <-chan producer.BytesStream
}

type reader interface {
	Read(ctx context.Context, stream <-chan producer.BytesStream) <-chan consumer.Result
}

type orch struct {
	producer streamer
	consumer reader
}

func New(producer streamer, consumer reader) *orch {
	return &orch{
		producer: producer,
		consumer: consumer,
	}
}

func (o *orch) Run(ctx context.Context) <-chan consumer.Result {
	filename := " def " // TODO
	var bpr int64 = 401

	stream := o.producer.Stream(ctx, filename, bpr)

	return o.consumer.Read(ctx, stream)

}
