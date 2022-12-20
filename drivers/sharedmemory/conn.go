// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
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
	"github.com/atomix/atomix/runtime/pkg/network"
	counterv1api "github.com/atomix/atomix/runtime/pkg/runtime/counter/v1"
	countermapv1api "github.com/atomix/atomix/runtime/pkg/runtime/countermap/v1"
	electionv1api "github.com/atomix/atomix/runtime/pkg/runtime/election/v1"
	indexedmapv1api "github.com/atomix/atomix/runtime/pkg/runtime/indexedmap/v1"
	lockv1api "github.com/atomix/atomix/runtime/pkg/runtime/lock/v1"
	mapv1api "github.com/atomix/atomix/runtime/pkg/runtime/map/v1"
	multimapv1api "github.com/atomix/atomix/runtime/pkg/runtime/multimap/v1"
	setv1api "github.com/atomix/atomix/runtime/pkg/runtime/set/v1"
	valuev1api "github.com/atomix/atomix/runtime/pkg/runtime/value/v1"
)

func newConn(network network.Driver) *memoryConn {
	return &memoryConn{
		ProtocolClient: client.NewClient(network),
	}
}

type memoryConn struct {
	*client.ProtocolClient
}

func (c *memoryConn) Connect(ctx context.Context, spec runtimev1.ConnSpec) error {
	var config rsmv1.ProtocolConfig
	if err := spec.UnmarshalConfig(&config); err != nil {
		return err
	}
	return c.ProtocolClient.Connect(ctx, config)
}

func (c *memoryConn) Configure(ctx context.Context, spec runtimev1.ConnSpec) error {
	var config rsmv1.ProtocolConfig
	if err := spec.UnmarshalConfig(&config); err != nil {
		return err
	}
	return c.ProtocolClient.Configure(ctx, config)
}

func (c *memoryConn) NewCounter(spec runtimev1.PrimitiveSpec) (counterv1api.Counter, error) {
	return counterv1.NewCounter(c.Protocol, spec)
}

func (c *memoryConn) NewCounterMap(spec runtimev1.PrimitiveSpec) (countermapv1api.CounterMap, error) {
	return countermapv1.NewCounterMap(c.Protocol, spec)
}

func (c *memoryConn) NewLeaderElection(spec runtimev1.PrimitiveSpec) (electionv1api.LeaderElection, error) {
	return electionv1.NewLeaderElection(c.Protocol, spec)
}

func (c *memoryConn) NewIndexedMap(spec runtimev1.PrimitiveSpec) (indexedmapv1api.IndexedMap, error) {
	return indexedmapv1.NewIndexedMap(c.Protocol, spec)
}

func (c *memoryConn) NewLock(spec runtimev1.PrimitiveSpec) (lockv1api.Lock, error) {
	return lockv1.NewLock(c.Protocol, spec)
}

func (c *memoryConn) NewMap(spec runtimev1.PrimitiveSpec) (mapv1api.Map, error) {
	return mapv1.NewMap(c.Protocol, spec)
}

func (c *memoryConn) NewMultiMap(spec runtimev1.PrimitiveSpec) (multimapv1api.MultiMap, error) {
	return multimapv1.NewMultiMap(c.Protocol, spec)
}

func (c *memoryConn) NewSet(spec runtimev1.PrimitiveSpec) (setv1api.Set, error) {
	return setv1.NewSet(c.Protocol, spec)
}

func (c *memoryConn) NewValue(spec runtimev1.PrimitiveSpec) (valuev1api.Value, error) {
	return valuev1.NewValue(c.Protocol, spec)
}
