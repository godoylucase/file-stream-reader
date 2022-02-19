package orch

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/stream"
	"github.com/godoylucase/s3-file-stream-reader/internal/concurrent/streamreader"
	"github.com/godoylucase/s3-file-stream-reader/internal/platform/awss3"
	"github.com/mitchellh/mapstructure"
)

const (
	TypeBucket    = "s3-bucket"
	TypeLocalFile = "local-file"
)

type streamer interface {
	Start(ctx context.Context) <-chan stream.RangeBytes
}

type reader interface {
	Process(ctx context.Context, stream <-chan stream.RangeBytes) <-chan streamreader.Data
}
type Orch struct {
	streamer streamer
	reader   reader
}

type Config struct {
	Type             string                 `mapstructure:"type"`
	ChunkByteSize    string                 `mapstructure:"chunk-size"`
	OnReadFnName     string                 `mapstructure:"reader-fn"`
	StreamersQty     string                 `mapstructure:"streamers"`
	ReadersQty       string                 `mapstructure:"readers"`
	LocationMetadata map[string]interface{} `mapstructure:",remain"`
}

func (conf *Config) filename() (string, error) {
	switch conf.Type {
	case TypeBucket:
		var loc S3Location
		if err := mapstructure.Decode(conf.LocationMetadata, &loc); err != nil {
			return "", err
		}
		return strings.Join([]string{loc.Bucket, loc.Key}, "/"), nil
	case TypeLocalFile:
		fallthrough
	default:
		return "", fmt.Errorf("the %v value is not a supported type", conf.Type)
	}
}

type S3Location struct {
	Bucket string `mapstructure:"bucket"`
	Key    string `mapstructure:"key"`
}

func FromConfig(conf *Config) (*Orch, error) {
	filename, err := conf.filename()
	if err != nil {
		return nil, err
	}

	src, err := source(conf.Type)
	if err != nil {
		return nil, err
	}

	sqty, err := strconv.Atoi(conf.StreamersQty)
	if err != nil {
		return nil, err
	}

	cbs, err := strconv.Atoi(conf.ChunkByteSize)
	if err != nil {
		return nil, err
	}

	s := stream.New(&stream.Config{
		Source:       src,
		StreamersQty: uint(sqty),
		BytesPerRead: int64(cbs),
		Filename:     filename,
	})

	rqty, err := strconv.Atoi(conf.ReadersQty)
	if err != nil {
		return nil, err
	}

	r := streamreader.New(&streamreader.Config{
		ReadersQty:   uint(rqty),
		OnReadFnName: conf.OnReadFnName,
	})

	return &Orch{
		streamer: s,
		reader:   r,
	}, nil
}

func (o *Orch) Run(ctx context.Context) <-chan streamreader.Data {
	strm := o.streamer.Start(ctx)
	return o.reader.Process(ctx, strm)
}

func source(typ string) (stream.Source, error) {
	var src stream.Source
	switch typ {
	case TypeBucket:
		s, err := awss3.NewProxy()
		if err != nil {
			return nil, err
		}
		src = s
	case TypeLocalFile:
		fallthrough
	default:
		fmt.Printf("the type %v is not supported", typ)
	}

	return src, nil
}
