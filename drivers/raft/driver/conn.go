// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	counterruntimev1 "github.com/atomix/atomix/api/runtime/counter/v1"
	countermapruntimev1 "github.com/atomix/atomix/api/runtime/countermap/v1"
	electionruntimev1 "github.com/atomix/atomix/api/runtime/election/v1"
	indexedmapruntimev1 "github.com/atomix/atomix/api/runtime/indexedmap/v1"
	lockruntimev1 "github.com/atomix/atomix/api/runtime/lock/v1"
	mapruntimev1 "github.com/atomix/atomix/api/runtime/map/v1"
	multimapruntimev1 "github.com/atomix/atomix/api/runtime/multimap/v1"
	setruntimev1 "github.com/atomix/atomix/api/runtime/set/v1"
	valueruntimev1 "github.com/atomix/atomix/api/runtime/value/v1"
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
)

func newConn(network network.Driver) *raftConn {
	return &raftConn{
		ProtocolClient: client.NewClient(network),
	}
}

type raftConn struct {
	*client.ProtocolClient
}

func (c *raftConn) Connect(ctx context.Context, config *rsmv1.ProtocolConfig) error {
	return c.ProtocolClient.Connect(ctx, *config)
}

func (c *raftConn) Configure(ctx context.Context, config *rsmv1.ProtocolConfig) error {
	return c.ProtocolClient.Configure(ctx, *config)
}

func (c *raftConn) NewCounterV1() counterruntimev1.CounterServer {
	return counterv1.NewCounter(c.Protocol)
}

func (c *raftConn) NewCounterMapV1() countermapruntimev1.CounterMapServer {
	return countermapv1.NewCounterMap(c.Protocol)
}

func (c *raftConn) NewLeaderElectionV1() electionruntimev1.LeaderElectionServer {
	return electionv1.NewLeaderElection(c.Protocol)
}

func (c *raftConn) NewIndexedMap(config *indexedmapprotocolv1.IndexedMapConfig) (indexedmapruntimev1.IndexedMapServer, error) {
	return indexedmapv1.NewIndexedMap(c.Protocol, config)
}

func (c *raftConn) NewLockV1() lockruntimev1.LockServer {
	return lockv1.NewLock(c.Protocol)
}

func (c *raftConn) NewMapV1(config *mapprotocolv1.MapConfig) (mapruntimev1.MapServer, error) {
	return mapv1.NewMap(c.Protocol, config)
}

func (c *raftConn) NewMultiMapV1() multimapruntimev1.MultiMapServer {
	return multimapv1.NewMultiMap(c.Protocol)
}

func (c *raftConn) NewSetV1() setruntimev1.SetServer {
	return setv1.NewSet(c.Protocol)
}

func (c *raftConn) NewValueV1() valueruntimev1.ValueServer {
	return valuev1.NewValue(c.Protocol)
}
