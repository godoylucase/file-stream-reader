package local

import (
	"os"
)

type localProxy struct {
}

func NewProxy() *localProxy {
	return &localProxy{}
}

func (l *localProxy) Length(filename string) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}

func (l *localProxy) Bytes(filename string, from int64, chunk []byte) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.ReadAt(chunk, from)
	if err != nil {
		return err
	}

	return nil
}
