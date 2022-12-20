// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	rsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	counternodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/counter/v1"
	countermapnodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/countermap/v1"
	electionnodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/election/v1"
	indexedmapnodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/indexedmap/v1"
	locknodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/lock/v1"
	mapnodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/map/v1"
	multimapnodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/multimap/v1"
	setnodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/set/v1"
	valuenodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/value/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	counterstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/counter/v1"
	countermapstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/countermap/v1"
	electionstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/election/v1"
	indexedmapstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/indexedmap/v1"
	lockstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/lock/v1"
	mapstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/map/v1"
	multimapstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/multimap/v1"
	setstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/set/v1"
	valuestatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/value/v1"
	"github.com/atomix/atomix/runtime/pkg/network"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"sync"
)

func newNode(network network.Driver, opts ...node.Option) *node.Node {
	node := node.NewNode(network, newProtocol(), opts...)
	counternodev1.RegisterServer(node)
	countermapnodev1.RegisterServer(node)
	electionnodev1.RegisterServer(node)
	indexedmapnodev1.RegisterServer(node)
	locknodev1.RegisterServer(node)
	mapnodev1.RegisterServer(node)
	multimapnodev1.RegisterServer(node)
	setnodev1.RegisterServer(node)
	valuenodev1.RegisterServer(node)
	return node
}

func newProtocol() node.Protocol {
	return &podMemoryProtocol{
		partition: node.NewPartition(1, newExecutor()),
	}
}

type podMemoryProtocol struct {
	partition node.Partition
}

func (p *podMemoryProtocol) Partitions() []node.Partition {
	return []node.Partition{p.partition}
}

func (p *podMemoryProtocol) Partition(partitionID rsmv1.PartitionID) (node.Partition, bool) {
	if p.partition.ID() != partitionID {
		return nil, false
	}
	return p.partition, true
}

func newExecutor() node.Executor {
	registry := statemachine.NewPrimitiveTypeRegistry()
	counterstatemachinev1.RegisterStateMachine(registry)
	countermapstatemachinev1.RegisterStateMachine(registry)
	electionstatemachinev1.RegisterStateMachine(registry)
	indexedmapstatemachinev1.RegisterStateMachine(registry)
	lockstatemachinev1.RegisterStateMachine(registry)
	mapstatemachinev1.RegisterStateMachine(registry)
	multimapstatemachinev1.RegisterStateMachine(registry)
	setstatemachinev1.RegisterStateMachine(registry)
	valuestatemachinev1.RegisterStateMachine(registry)
	return &podMemoryExecutor{
		sm: statemachine.NewStateMachine(registry),
	}
}

type podMemoryExecutor struct {
	sm statemachine.StateMachine
	mu sync.RWMutex
}

func (e *podMemoryExecutor) Propose(ctx context.Context, proposal *rsmv1.ProposalInput, stream streams.WriteStream[*rsmv1.ProposalOutput]) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sm.Propose(proposal, stream)
	return nil
}

func (e *podMemoryExecutor) Query(ctx context.Context, query *rsmv1.QueryInput, stream streams.WriteStream[*rsmv1.QueryOutput]) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	e.sm.Query(query, stream)
	return nil
}
