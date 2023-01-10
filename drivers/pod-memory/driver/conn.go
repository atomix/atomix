// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	counterapiv1 "github.com/atomix/atomix/api/runtime/counter/v1"
	countermapapiv1 "github.com/atomix/atomix/api/runtime/countermap/v1"
	electionapiv1 "github.com/atomix/atomix/api/runtime/election/v1"
	indexedmapapiv1 "github.com/atomix/atomix/api/runtime/indexedmap/v1"
	lockapiv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	mapapiv1 "github.com/atomix/atomix/api/runtime/map/v1"
	multimapapiv1 "github.com/atomix/atomix/api/runtime/multimap/v1"
	setapiv1 "github.com/atomix/atomix/api/runtime/set/v1"
	valueapiv1 "github.com/atomix/atomix/api/runtime/value/v1"
	indexedmaprsmv1 "github.com/atomix/atomix/protocols/rsm/api/indexedmap/v1"
	maprsmv1 "github.com/atomix/atomix/protocols/rsm/api/map/v1"
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
	"strconv"
	"strings"
	"sync"
)

func newConn(network network.Driver) *podMemoryConn {
	return &podMemoryConn{
		ProtocolClient: client.NewClient(network),
	}
}

type podMemoryConn struct {
	*client.ProtocolClient
	network network.Driver
	node    *node.Node
	mu      sync.Mutex
}

func (c *podMemoryConn) Connect(ctx context.Context, spec rsmapiv1.ProtocolConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.node != nil {
		return nil
	}

	parts := strings.Split(spec.Partitions[0].Leader, ":")
	host, portS := parts[0], parts[1]
	port, err := strconv.Atoi(portS)
	if err != nil {
		return errors.NewInvalid(err.Error())
	}

	c.node = newNode(c.network,
		node.WithHost(host),
		node.WithPort(port))
	if err := c.node.Start(); err != nil {
		return err
	}
	return c.ProtocolClient.Connect(ctx, spec)
}

func (c *podMemoryConn) NewCounterV1() counterapiv1.CounterServer {
	return counterclientv1.NewCounter(c.Protocol)
}

func (c *podMemoryConn) NewCounterMapV1() countermapapiv1.CounterMapServer {
	return countermapclientv1.NewCounterMap(c.Protocol)
}

func (c *podMemoryConn) NewLeaderElectionV1() electionapiv1.LeaderElectionServer {
	return electionclientv1.NewLeaderElection(c.Protocol)
}

func (c *podMemoryConn) NewIndexedMapV1(spec *indexedmaprsmv1.IndexedMapConfig) (indexedmapapiv1.IndexedMapServer, error) {
	return indexedmapclientv1.NewIndexedMap(c.Protocol, spec)
}

func (c *podMemoryConn) NewLockV1() lockapiv1.LockServer {
	return lockclientv1.NewLock(c.Protocol)
}

func (c *podMemoryConn) NewMapV1(spec *maprsmv1.MapConfig) (mapapiv1.MapServer, error) {
	return mapclientv1.NewMap(c.Protocol, spec)
}

func (c *podMemoryConn) NewMultiMapV1() multimapapiv1.MultiMapServer {
	return multimapclientv1.NewMultiMap(c.Protocol)
}

func (c *podMemoryConn) NewSetV1() setapiv1.SetServer {
	return setclientv1.NewSet(c.Protocol)
}

func (c *podMemoryConn) NewValueV1() valueapiv1.ValueServer {
	return valueclientv1.NewValue(c.Protocol)
}
