package fsource

import (
	"fmt"
)

const (
	TypeBucket    = "s3-bucket"
	TypeLocalFile = "local-file"
)

type FileSource interface {
	Length(filename string) (int64, error)
	GetBytes(filename string, from int64, chunk []byte) error
}

func Get(typ string) (FileSource, error) {
	switch typ {
	case TypeBucket:
		p, err := NewS3()
		if err != nil {
			return nil, err
		}
		return p, nil
	case TypeLocalFile:
		return newLocal(), nil
	default:
		// TODO include a registry where to lookup external customized sources
		return nil, fmt.Errorf("the type %v is not supported", typ)
	}
}
