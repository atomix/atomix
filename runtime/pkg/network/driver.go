// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package network

import (
	"context"
	"io"
	"net"
	"sync"

	"google.golang.org/grpc/test/bufconn"
)

// Driver is a network driver.
type Driver interface {
	// Listen creates a new Listener
	Listen(address string) (net.Listener, error)
	// Connect creates a new Conn
	Connect(ctx context.Context, address string) (net.Conn, error)
}

// NewDefaultDriver creates a new physical Driver
func NewDefaultDriver() Driver {
	return &defaultDriver{}
}

// defaultDriver is a physical network driver
type defaultDriver struct{}

func (n *defaultDriver) Listen(address string) (net.Listener, error) {
	return net.Listen("tcp", address)
}

func (n *defaultDriver) Connect(ctx context.Context, address string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", address)
}

const localBufSize = 1024 * 1024

// NewLocalDriver creates a new process-local Driver
func NewLocalDriver() Driver {
	return &localDriver{
		listeners: make(map[string]*bufconn.Listener),
		watchers:  make(map[string][]chan<- *bufconn.Listener),
	}
}

type localDriver struct {
	listeners   map[string]*bufconn.Listener
	listenersMu sync.Mutex
	watchers    map[string][]chan<- *bufconn.Listener
	watchersMu  sync.RWMutex
}

func (n *localDriver) Listen(address string) (net.Listener, error) {
	lis := bufconn.Listen(localBufSize)
	n.listenersMu.Lock()
	n.listeners[address] = lis
	n.listenersMu.Unlock()

	n.watchersMu.Lock()
	watchers := n.watchers[address]
	delete(n.watchers, address)
	n.watchersMu.Unlock()

	for _, watcher := range watchers {
		watcher <- lis
		close(watcher)
	}
	return lis, nil
}

func (n *localDriver) Connect(ctx context.Context, address string) (net.Conn, error) {
	n.listenersMu.Lock()
	lis, ok := n.listeners[address]
	n.listenersMu.Unlock()
	if ok {
		return lis.Dial()
	}

	n.watchersMu.Lock()
	watcher := make(chan *bufconn.Listener)
	watchers := n.watchers[address]
	watchers = append(watchers, watcher)
	n.watchers[address] = watchers
	n.watchersMu.Unlock()

	select {
	case lis, ok := <-watcher:
		if !ok {
			return nil, io.EOF
		}
		return lis.Dial()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
