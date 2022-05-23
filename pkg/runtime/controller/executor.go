// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/runtime/store"
	"github.com/atomix/runtime/pkg/service"
	"github.com/cenkalti/backoff"
	"sync"
	"time"
)

var log = logging.GetLogger()

type Reconciler[I runtimev1.ObjectID] interface {
	Reconcile(id I) error
}

type Executor interface {
	service.Service
}

func NewExecutor[I runtimev1.ObjectID](reconciler Reconciler[I], watchers ...Watcher[I]) Executor {
	return &runtimeController[I]{
		reconciler: reconciler,
		watchers:   watchers,
	}
}

type runtimeController[I runtimev1.ObjectID] struct {
	reconciler Reconciler[I]
	watchers   []Watcher[I]
	watches    map[store.WatchID]Watcher[I]
	mu         sync.Mutex
}

func (c *runtimeController[I]) Start() error {
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

func (c *runtimeController[I]) Stop() error {
	for watcherID, watchable := range c.watches {
		watchable.Unwatch(watcherID)
	}
	return nil
}
