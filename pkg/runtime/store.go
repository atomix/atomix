// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"sync"
)

type Store[T runtimev1.Object] interface {
	Get(id runtimev1.ObjectId) (T, error)
	List() ([]T, error)
	Create(object T) error
	Update(object T) error
	Delete(object T) error
}

func NewStore[T runtimev1.Object]() Store[T] {
	return &localStore[T]{
		objects: make(map[runtimev1.ObjectId]T),
	}
}

type localStore[T runtimev1.Object] struct {
	objects  map[runtimev1.ObjectId]T
	revision runtimev1.ObjectRevision
	mu       sync.RWMutex
}

func (s *localStore[T]) Get(id runtimev1.ObjectId) (T, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	object, ok := s.objects[id]
	if !ok {
		return object, errors.NewNotFound("object '%s' not found", id)
	}
	return object, nil
}

func (s *localStore[T]) List() ([]T, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	objects := make([]T, 0, len(s.objects))
	for _, object := range s.objects {
		objects = append(objects, object)
	}
	return objects, nil
}

func (s *localStore[T]) Create(object T) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := object.GetMeta().ID
	meta := object.GetMeta()

	if _, ok := s.objects[id]; ok {
		return errors.NewAlreadyExists("object '%s' already exists", id)
	}
	s.revision++
	meta.Revision = s.revision
	object.SetMeta(meta)
	s.objects[id] = object
	return nil
}

func (s *localStore[T]) Update(object T) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := object.GetMeta().ID
	meta := object.GetMeta()

	stored, ok := s.objects[id]
	if !ok {
		return errors.NewNotFound("object '%s' not found", id)
	}
	if meta.Revision != object.GetMeta().Revision {
		return errors.NewConflict("object revision does not match the stored revision %d", stored.GetMeta().Revision)
	}
	s.revision++
	meta.Revision = s.revision
	object.SetMeta(meta)
	s.objects[id] = object
	return nil
}

func (s *localStore[T]) Delete(object T) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := object.GetMeta().ID
	meta := object.GetMeta()

	stored, ok := s.objects[id]
	if !ok {
		return errors.NewNotFound("object '%s' not found", id)
	}
	if meta.Revision != object.GetMeta().Revision {
		return errors.NewConflict("object revision does not match the stored revision %d", stored.GetMeta().Revision)
	}
	delete(s.objects, id)
	return nil
}

var _ Store[runtimev1.Object] = (*localStore[runtimev1.Object])(nil)
