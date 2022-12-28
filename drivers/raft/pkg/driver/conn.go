// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	indexedmapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/indexedmap/v1"
	mapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/map/v1"
	rsmv1 "github.com/atomix/atomix/protocols/rsm/api/v1"
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
	counterruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/counter/v1"
	countermapruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/countermap/v1"
	electionruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/election/v1"
	indexedmapruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/indexedmap/v1"
	lockruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/lock/v1"
	mapruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/map/v1"
	multimapruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/multimap/v1"
	setruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/set/v1"
	valueruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/value/v1"
)

func newConn(network network.Driver) *multiRaftConn {
	return &multiRaftConn{
		ProtocolClient: client.NewClient(network),
	}
}

type multiRaftConn struct {
	*client.ProtocolClient
}

func (c *multiRaftConn) Connect(ctx context.Context, config *rsmv1.ProtocolConfig) error {
	return c.ProtocolClient.Connect(ctx, *config)
}

func (c *multiRaftConn) Configure(ctx context.Context, config *rsmv1.ProtocolConfig) error {
	return c.ProtocolClient.Configure(ctx, *config)
}

func (c *multiRaftConn) NewCounter() (counterruntimev1.Counter, error) {
	return counterv1.NewCounter(c.Protocol)
}

func (c *multiRaftConn) NewCounterMap() (countermapruntimev1.CounterMap, error) {
	return countermapv1.NewCounterMap(c.Protocol)
}

func (c *multiRaftConn) NewLeaderElection() (electionruntimev1.LeaderElection, error) {
	return electionv1.NewLeaderElection(c.Protocol)
}

func (c *multiRaftConn) NewIndexedMap(config *indexedmapprotocolv1.IndexedMapConfig) (indexedmapruntimev1.IndexedMap, error) {
	return indexedmapv1.NewIndexedMap(c.Protocol, *config)
}

func (c *multiRaftConn) NewLock() (lockruntimev1.Lock, error) {
	return lockv1.NewLock(c.Protocol)
}

func (c *multiRaftConn) NewMap(config *mapprotocolv1.MapConfig) (mapruntimev1.Map, error) {
	return mapv1.NewMap(c.Protocol, config)
}

func (c *multiRaftConn) NewMultiMap() (multimapruntimev1.MultiMap, error) {
	return multimapv1.NewMultiMap(c.Protocol)
}

func (c *multiRaftConn) NewSet() (setruntimev1.Set, error) {
	return setv1.NewSet(c.Protocol)
}

func (c *multiRaftConn) NewValue() (valueruntimev1.Value, error) {
	return valuev1.NewValue(c.Protocol)
}
