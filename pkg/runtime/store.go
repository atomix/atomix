// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"github.com/golang/protobuf/proto"
	"sync"
)

type Store[I comparable, T proto.Message] interface {
	Get(id I) (T, error)
	Create(value T) error
	Update(value T) error
	Delete(value T) error
}

type IDProvider[I comparable, T proto.Message] func(T) I

func NewStore[I comparable, T proto.Message](idProvider IDProvider[I, T]) Store[I, T] {
	return &localStore[I, T]{
		idProvider: idProvider,
		objects:    make(map[I]T),
	}
}

type localStore[I comparable, T proto.Message] struct {
	idProvider IDProvider[I, T]
	objects    map[I]T
	mu         sync.RWMutex
}

func (s *localStore[I, T]) Get(id I) (T, error) {
	//TODO implement me
	panic("implement me")
}

func (s *localStore[I, T]) Create(value T) error {
	//TODO implement me
	panic("implement me")
}

func (s *localStore[I, T]) Update(value T) error {
	//TODO implement me
	panic("implement me")
}

func (s *localStore[I, T]) Delete(value T) error {
	//TODO implement me
	panic("implement me")
}

var _ Store[any, proto.Message] = (*localStore[any, proto.Message])(nil)
