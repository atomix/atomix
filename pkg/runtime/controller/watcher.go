// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/runtime/store"
)

type Watcher[I runtimev1.ObjectID] interface {
	Watch(chan<- I) store.WatchID
	Unwatch(store.WatchID)
}

func NewWatcher[I runtimev1.ObjectID, J runtimev1.ObjectID, T runtimev1.Object[J]](watchable store.Watchable[J, T], f func(T) []I) Watcher[I] {
	return &watchableWatcher[I, J, T]{
		watchable: watchable,
		f:         f,
	}
}

type watchableWatcher[I runtimev1.ObjectID, J runtimev1.ObjectID, T runtimev1.Object[J]] struct {
	watchable store.Watchable[J, T]
	f         func(T) []I
}

func (w *watchableWatcher[I, J, T]) Watch(ch chan<- I) store.WatchID {
	watchCh := make(chan T)
	go func() {
		for object := range watchCh {
			objectIDs := w.f(object)
			for _, objectID := range objectIDs {
				ch <- objectID
			}
		}
		close(ch)
	}()
	return w.watchable.Watch(watchCh)
}

func (w *watchableWatcher[I, J, T]) Unwatch(watchID store.WatchID) {
	w.watchable.Unwatch(watchID)
}
