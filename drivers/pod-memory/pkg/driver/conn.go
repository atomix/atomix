// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	rsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	counterv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/counter/v1"
	countermapv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/countermap/v1"
	electionv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/election/v1"
	indexedmapv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/indexedmap/v1"
	lockv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/lock/v1"
	mapv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/map/v1"
	multimapv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/multimap/v1"
	setv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/set/v1"
	valuev1 "github.com/atomix/atomix/protocols/rsm/pkg/client/value/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/runtime/pkg/network"
	counterruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/counter/v1"
	countermapruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/countermap/v1"
	electionruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/election/v1"
	indexedmapruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/indexedmap/v1"
	lockruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/lock/v1"
	mapruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/map/v1"
	multimapruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/multimap/v1"
	setruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/set/v1"
	valueruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/value/v1"
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

func (c *podMemoryConn) Connect(ctx context.Context, spec runtimev1.ConnSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.node != nil {
		return nil
	}

	c.node = newNode(c.network,
		node.WithHost(fmt.Sprintf("%s.%s", spec.StoreID.Namespace, spec.StoreID.Name)),
		node.WithPort(8080))
	if err := c.node.Start(); err != nil {
		return err
	}

	config := rsmv1.ProtocolConfig{
		Partitions: []rsmv1.PartitionConfig{
			{
				PartitionID: 1,
				Leader:      fmt.Sprintf("%s.%s:8080", spec.StoreID.Namespace, spec.StoreID.Name),
			},
		},
	}
	return c.ProtocolClient.Connect(ctx, config)
}

func (c *podMemoryConn) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.node == nil {
		return nil
	}
	if err := c.ProtocolClient.Close(ctx); err != nil {
		return err
	}
	if err := c.node.Stop(); err != nil {
		return err
	}
	return nil
}

func (c *podMemoryConn) NewCounter(spec runtimev1.PrimitiveSpec) (counterruntimev1.Counter, error) {
	return counterv1.NewCounter(c.Protocol, spec)
}

func (c *podMemoryConn) NewCounterMap(spec runtimev1.PrimitiveSpec) (countermapruntimev1.CounterMap, error) {
	return countermapv1.NewCounterMap(c.Protocol, spec)
}

func (c *podMemoryConn) NewLeaderElection(spec runtimev1.PrimitiveSpec) (electionruntimev1.LeaderElection, error) {
	return electionv1.NewLeaderElection(c.Protocol, spec)
}

func (c *podMemoryConn) NewIndexedMap(spec runtimev1.PrimitiveSpec) (indexedmapruntimev1.IndexedMap, error) {
	return indexedmapv1.NewIndexedMap(c.Protocol, spec)
}

func (c *podMemoryConn) NewLock(spec runtimev1.PrimitiveSpec) (lockruntimev1.Lock, error) {
	return lockv1.NewLock(c.Protocol, spec)
}

func (c *podMemoryConn) NewMap(spec runtimev1.PrimitiveSpec) (mapruntimev1.Map, error) {
	return mapv1.NewMap(c.Protocol, spec)
}

func (c *podMemoryConn) NewMultiMap(spec runtimev1.PrimitiveSpec) (multimapruntimev1.MultiMap, error) {
	return multimapv1.NewMultiMap(c.Protocol, spec)
}

func (c *podMemoryConn) NewSet(spec runtimev1.PrimitiveSpec) (setruntimev1.Set, error) {
	return setv1.NewSet(c.Protocol, spec)
}

func (c *podMemoryConn) NewValue(spec runtimev1.PrimitiveSpec) (valueruntimev1.Value, error) {
	return valuev1.NewValue(c.Protocol, spec)
}
