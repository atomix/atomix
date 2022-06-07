// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package store

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"sync"
)

type Provider[I runtimev1.ObjectID, T runtimev1.Object[I]] interface {
	Store() *Store[I, T]
}

type WatchID uuid.UUID

type Watchable[I runtimev1.ObjectID, T runtimev1.Object[I]] interface {
	Watch(chan<- T) WatchID
	Unwatch(WatchID)
}

func NewStore[I runtimev1.ObjectID, T runtimev1.Object[I]]() *Store[I, T] {
	store := &Store[I, T]{
		objects:  make(map[string]T),
		watchCh:  make(chan T),
		watchers: make(map[WatchID]chan<- T),
	}
	store.open()
	return store
}

type Store[I runtimev1.ObjectID, T runtimev1.Object[I]] struct {
	objects  map[string]T
	version  runtimev1.ObjectVersion
	watchCh  chan T
	watchers map[WatchID]chan<- T
	mu       sync.RWMutex
}

func (s *Store[I, T]) open() {
	go s.propagate()
}

func (s *Store[I, T]) propagate() {
	for object := range s.watchCh {
		s.mu.RLock()
		for _, watcher := range s.watchers {
			watcher <- object
		}
		s.mu.RUnlock()
	}
	s.mu.RLock()
	for _, ch := range s.watchers {
		close(ch)
	}
	s.mu.RUnlock()
}

func (s *Store[I, T]) publish(object T) {
	s.watchCh <- proto.Clone(object).(T)
}

func (s *Store[I, T]) Get(id I) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	object, ok := s.objects[id.String()]
	return object, ok
}

func (s *Store[I, T]) List() []T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	objects := make([]T, 0, len(s.objects))
	for _, object := range s.objects {
		objects = append(objects, object)
	}
	return objects
}

func (s *Store[I, T]) Create(object T) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := object.GetID()
	if _, ok := s.objects[id.String()]; ok {
		return errors.NewAlreadyExists("object '%s' already exists", id)
	}
	s.version++
	object.SetVersion(s.version)
	s.objects[id.String()] = object
	s.publish(object)
	return nil
}

func (s *Store[I, T]) Update(object T) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := object.GetID()
	stored, ok := s.objects[id.String()]
	if !ok {
		return errors.NewNotFound("object '%s' not found", id)
	}
	if object.GetVersion() != stored.GetVersion() {
		return errors.NewConflict("object revision does not match the stored revision %d", stored.GetVersion())
	}
	s.version++
	object.SetVersion(s.version)
	s.objects[id.String()] = object
	s.publish(object)
	return nil
}

func (s *Store[I, T]) Delete(object T) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := object.GetID()
	stored, ok := s.objects[id.String()]
	if !ok {
		return errors.NewNotFound("object '%s' not found", id)
	}
	if object.GetVersion() != stored.GetVersion() {
		return errors.NewConflict("object revision does not match the stored revision %d", stored.GetVersion())
	}
	delete(s.objects, id.String())
	s.publish(object)
	return nil
}

func (s *Store[I, T]) Watch(watcher chan<- T) WatchID {
	s.mu.Lock()
	defer s.mu.Unlock()
	watcherID := WatchID(uuid.New())
	s.watchers[watcherID] = watcher
	return watcherID
}

func (s *Store[I, T]) Unwatch(watcherID WatchID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	watcher, ok := s.watchers[watcherID]
	if ok {
		delete(s.watchers, watcherID)
		close(watcher)
	}
}

func (s *Store[I, T]) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.watchCh)
}
