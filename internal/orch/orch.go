package orch

import (
	"context"
	"fmt"
	"strings"

	"github.com/godoylucase/s3-file-stream-reader/internal/fstream"
	"github.com/godoylucase/s3-file-stream-reader/internal/sread"
	"github.com/godoylucase/s3-file-stream-reader/platform/awss3"
	"github.com/mitchellh/mapstructure"
)

const (
	TypeBucket    = "s3-bucket"
	TypeLocalFile = "local-file"
)

type streamer interface {
	Start(ctx context.Context) <-chan fstream.Chunk
}

type reader interface {
	Process(ctx context.Context, stream <-chan fstream.Chunk) <-chan sread.Data
}

type orchestrator struct {
	streamer streamer
	reader   reader
}

type Config struct {
	Type       string
	StreamConf *StreamConf
	ReadConf   *ReadConf
}

type StreamConf struct {
	ChunkSize        int64
	Qty              uint
	LocationMetadata map[string]interface{}
}

type ReadConf struct {
	Qty          uint
	OnReadFnName string
}

func (conf *Config) filename() (string, error) {
	switch conf.Type {
	case TypeBucket:
		var loc S3Location
		if err := mapstructure.Decode(conf.StreamConf.LocationMetadata, &loc); err != nil {
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

func FromConfig(conf *Config) (*orchestrator, error) {
	filename, err := conf.filename()
	if err != nil {
		return nil, err
	}

	src, err := source(conf.Type)
	if err != nil {
		return nil, err
	}

	s := fstream.New(
		src,
		&fstream.WithConfig{
			Streamers: conf.StreamConf.Qty,
			ChunkSize: conf.StreamConf.ChunkSize,
			Filename:  filename,
		})

	r := sread.New(
		&sread.WithConfig{
			ReadersQty:   conf.ReadConf.Qty,
			OnReadFnName: conf.ReadConf.OnReadFnName,
		})

	return &orchestrator{
		streamer: s,
		reader:   r,
	}, nil
}

func (o *orchestrator) Run(ctx context.Context) <-chan sread.Data {
	return o.reader.Process(ctx, o.streamer.Start(ctx))
}

func source(typ string) (fstream.FileSource, error) {
	var src fstream.FileSource
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
