package sread

import (
	"fmt"
	"sync"
)

var registry = sync.Map{}

type onStreamRead func([]byte) (interface{}, error)

func RegisterFn(name string, fn func([]byte) (interface{}, error)) {
	fmt.Printf("registering on read function with name: %+v\n", name)
	registry.Store(name, fn)
}

func onReadFn(name string) (onStreamRead, error) {
	load, ok := registry.Load(name)
	if !ok {
		return nil, fmt.Errorf("%v is not a valid on read function", name)
	}

	fn, ok := load.(func([]byte) (interface{}, error))
	if !ok {
		return nil, fmt.Errorf("%v is not a valid on read function", name)
	}

	return fn, nil
}
