package sread

import (
	"errors"
	"fmt"
	"sync"
)

var (
	registry      = sync.Map{}
	ErrNotValidFn = errors.New("not valid function name")
)

type onStreamRead func([]byte) (interface{}, error)

func RegisterFn(name string, fn func([]byte) (interface{}, error)) {
	fmt.Printf("registering on read function with name: %+v\n", name)
	registry.Store(name, fn)
}

func onReadFn(name string) (onStreamRead, error) {
	load, ok := registry.Load(name)
	if !ok {
		return nil, ErrNotValidFn
	}

	fn, ok := load.(func([]byte) (interface{}, error))
	if !ok {
		return nil, ErrNotValidFn
	}

	return fn, nil
}
