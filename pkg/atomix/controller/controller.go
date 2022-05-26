// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/store"
	"github.com/cenkalti/backoff"
	"sync"
	"time"
)

var log = logging.GetLogger()

type Provider[I runtimev1.ObjectID] interface {
	Controller() *Controller[I]
}

type Reconciler[I runtimev1.ObjectID] interface {
	Reconcile(id I) error
}

func NewController[I runtimev1.ObjectID](reconciler Reconciler[I], watchers ...Watcher[I]) *Controller[I] {
	return &Controller[I]{
		reconciler: reconciler,
		watchers:   watchers,
	}
}

type Controller[I runtimev1.ObjectID] struct {
	reconciler Reconciler[I]
	watchers   []Watcher[I]
	watches    map[store.WatchID]Watcher[I]
	mu         sync.Mutex
}

func (c *Controller[I]) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.watches = make(map[store.WatchID]Watcher[I])

	ch := make(chan I)
	wg := &sync.WaitGroup{}
	for _, watchable := range c.watchers {
		watchCh := make(chan I)
		watcherID := watchable.Watch(watchCh)
		c.watches[watcherID] = watchable

		wg.Add(1)
		go func(watchCh <-chan I) {
			for objectID := range watchCh {
				ch <- objectID
			}
			wg.Done()
		}(watchCh)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	go func() {
		for objectID := range ch {
			_ = backoff.RetryNotify(func() error {
				return c.reconciler.Reconcile(objectID)
			}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
				log.Warn(err)
			})
		}
	}()
	return nil
}

func (c *Controller[I]) Stop() error {
	for watcherID, watchable := range c.watches {
		watchable.Unwatch(watcherID)
	}
	return nil
}
