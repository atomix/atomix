// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	rsmapiv1 "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	counterclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/counter/v1"
	countermapclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/countermap/v1"
	electionclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/election/v1"
	indexedmapclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/indexedmap/v1"
	lockclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/lock/v1"
	mapclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/map/v1"
	multimapclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/multimap/v1"
	setclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/set/v1"
	valueclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/value/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/runtime/pkg/network"
	runtimecounterv1 "github.com/atomix/atomix/runtime/pkg/runtime/counter/v1"
	runtimecountermapv1 "github.com/atomix/atomix/runtime/pkg/runtime/countermap/v1"
	runtimeelectionv1 "github.com/atomix/atomix/runtime/pkg/runtime/election/v1"
	runtimeindexedmapv1 "github.com/atomix/atomix/runtime/pkg/runtime/indexedmap/v1"
	runtimelockv1 "github.com/atomix/atomix/runtime/pkg/runtime/lock/v1"
	runtimemapv1 "github.com/atomix/atomix/runtime/pkg/runtime/map/v1"
	runtimemultimapv1 "github.com/atomix/atomix/runtime/pkg/runtime/multimap/v1"
	runtimesetv1 "github.com/atomix/atomix/runtime/pkg/runtime/set/v1"
	runtimevaluev1 "github.com/atomix/atomix/runtime/pkg/runtime/value/v1"
	"sync"
)

func newConn(network network.Driver) *podMemoryConn {
	return &podMemoryConn{
		ProtocolClient: client.NewClient(network),
		network:        network,
	}
}

type podMemoryConn struct {
	*client.ProtocolClient
	network network.Driver
	node    *node.Node
	mu      sync.Mutex
}

func (c *podMemoryConn) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.node != nil {
		return nil
	}

	c.node = newNode(c.network)
	if err := c.node.Start(); err != nil {
		return err
	}
	config := rsmapiv1.ProtocolConfig{
		Partitions: []rsmapiv1.PartitionConfig{
			{
				PartitionID: 1,
				Leader:      fmt.Sprintf("%s:%d", c.node.Host, c.node.Port),
			},
		},
	}
	return c.ProtocolClient.Connect(ctx, config)
}

func (c *podMemoryConn) NewCounterV1(ctx context.Context, id runtimev1.PrimitiveID) (runtimecounterv1.CounterProxy, error) {
	proxy := counterclientv1.NewCounter(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *podMemoryConn) NewCounterMapV1(ctx context.Context, id runtimev1.PrimitiveID) (runtimecountermapv1.CounterMapProxy, error) {
	proxy := countermapclientv1.NewCounterMap(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *podMemoryConn) NewLeaderElectionV1(ctx context.Context, id runtimev1.PrimitiveID) (runtimeelectionv1.LeaderElectionProxy, error) {
	proxy := electionclientv1.NewLeaderElection(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *podMemoryConn) NewIndexedMapV1(ctx context.Context, id runtimev1.PrimitiveID) (runtimeindexedmapv1.IndexedMapProxy, error) {
	proxy := indexedmapclientv1.NewIndexedMap(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *podMemoryConn) NewLockV1(ctx context.Context, id runtimev1.PrimitiveID) (runtimelockv1.LockProxy, error) {
	proxy := lockclientv1.NewLock(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *podMemoryConn) NewMapV1(ctx context.Context, id runtimev1.PrimitiveID) (runtimemapv1.MapProxy, error) {
	proxy := mapclientv1.NewMap(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *podMemoryConn) NewMultiMapV1(ctx context.Context, id runtimev1.PrimitiveID) (runtimemultimapv1.MultiMapProxy, error) {
	proxy := multimapclientv1.NewMultiMap(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *podMemoryConn) NewSetV1(ctx context.Context, id runtimev1.PrimitiveID) (runtimesetv1.SetProxy, error) {
	proxy := setclientv1.NewSet(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *podMemoryConn) NewValueV1(ctx context.Context, id runtimev1.PrimitiveID) (runtimevaluev1.ValueProxy, error) {
	proxy := valueclientv1.NewValue(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

var _ runtimecounterv1.CounterProvider = (*podMemoryConn)(nil)
var _ runtimecountermapv1.CounterMapProvider = (*podMemoryConn)(nil)
var _ runtimeelectionv1.LeaderElectionProvider = (*podMemoryConn)(nil)
var _ runtimeindexedmapv1.IndexedMapProvider = (*podMemoryConn)(nil)
var _ runtimelockv1.LockProvider = (*podMemoryConn)(nil)
var _ runtimemapv1.MapProvider = (*podMemoryConn)(nil)
var _ runtimemultimapv1.MultiMapProvider = (*podMemoryConn)(nil)
var _ runtimesetv1.SetProvider = (*podMemoryConn)(nil)
var _ runtimevaluev1.ValueProvider = (*podMemoryConn)(nil)
