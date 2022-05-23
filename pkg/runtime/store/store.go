// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package store

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"sync"
)

type WatchID uuid.UUID

type Watchable[I runtimev1.ObjectID, T runtimev1.Object[I]] interface {
	Watch(chan<- T) WatchID
	Unwatch(WatchID)
}

type Store[I runtimev1.ObjectID, T runtimev1.Object[I]] interface {
	Watchable[I, T]
	Get(id I) (T, bool)
	List() []T
	Create(object T) error
	Update(object T) error
	Delete(object T) error
	close()
}

func NewStore[I runtimev1.ObjectID, T runtimev1.Object[I]]() Store[I, T] {
	store := &localStore[I, T]{
		objects:  make(map[string]T),
		watchCh:  make(chan T),
		watchers: make(map[WatchID]chan<- T),
	}
	store.open()
	return store
}

type localStore[I runtimev1.ObjectID, T runtimev1.Object[I]] struct {
	objects  map[string]T
	version  runtimev1.ObjectVersion
	watchCh  chan T
	watchers map[WatchID]chan<- T
	mu       sync.RWMutex
}

func (s *localStore[I, T]) open() {
	go s.propagate()
}

func (s *localStore[I, T]) propagate() {
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

func (s *localStore[I, T]) publish(object T) {
	s.watchCh <- proto.Clone(object).(T)
}

func (s *localStore[I, T]) Get(id I) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	object, ok := s.objects[id.String()]
	return object, ok
}

func (s *localStore[I, T]) List() []T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	objects := make([]T, 0, len(s.objects))
	for _, object := range s.objects {
		objects = append(objects, object)
	}
	return objects
}

func (s *localStore[I, T]) Create(object T) error {
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

func (s *localStore[I, T]) Update(object T) error {
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

func (s *localStore[I, T]) Delete(object T) error {
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

func (s *localStore[I, T]) Watch(watcher chan<- T) WatchID {
	s.mu.Lock()
	defer s.mu.Unlock()
	watcherID := WatchID(uuid.New())
	s.watchers[watcherID] = watcher
	return watcherID
}

func (s *localStore[I, T]) Unwatch(watcherID WatchID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	watcher, ok := s.watchers[watcherID]
	if ok {
		delete(s.watchers, watcherID)
		close(watcher)
	}
}

func (s *localStore[I, T]) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.watchCh)
}

var _ Store[runtimev1.ObjectID, runtimev1.Object[runtimev1.ObjectID]] = (*localStore[runtimev1.ObjectID, runtimev1.Object[runtimev1.ObjectID]])(nil)
