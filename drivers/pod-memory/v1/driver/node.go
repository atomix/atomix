// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	rsmapiv1 "github.com/atomix/atomix/protocols/rsm/api/v1"
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
	countersmv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/counter/v1"
	countermapsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/countermap/v1"
	electionsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/election/v1"
	indexedmapsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/indexedmap/v1"
	locksmv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/lock/v1"
	mapsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/map/v1"
	multimapsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/multimap/v1"
	setsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/set/v1"
	valuesmv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/value/v1"
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
		partitions: map[rsmapiv1.PartitionID]node.Partition{
			1: node.NewPartition(1, newExecutor()),
			2: node.NewPartition(2, newExecutor()),
			3: node.NewPartition(3, newExecutor()),
		},
	}
}

type podMemoryProtocol struct {
	partitions map[rsmapiv1.PartitionID]node.Partition
}

func (p *podMemoryProtocol) Partitions() []node.Partition {
	partitions := make([]node.Partition, 0, len(p.partitions))
	for _, partition := range p.partitions {
		partitions = append(partitions, partition)
	}
	return partitions
}

func (p *podMemoryProtocol) Partition(partitionID rsmapiv1.PartitionID) (node.Partition, bool) {
	partition, ok := p.partitions[partitionID]
	return partition, ok
}

func newExecutor() node.Executor {
	registry := statemachine.NewPrimitiveTypeRegistry()
	countersmv1.RegisterStateMachine(registry)
	countermapsmv1.RegisterStateMachine(registry)
	electionsmv1.RegisterStateMachine(registry)
	indexedmapsmv1.RegisterStateMachine(registry)
	locksmv1.RegisterStateMachine(registry)
	mapsmv1.RegisterStateMachine(registry)
	multimapsmv1.RegisterStateMachine(registry)
	setsmv1.RegisterStateMachine(registry)
	valuesmv1.RegisterStateMachine(registry)
	return &podMemoryExecutor{
		sm: statemachine.NewStateMachine(registry),
	}
}

type podMemoryExecutor struct {
	sm statemachine.StateMachine
	mu sync.RWMutex
}

func (e *podMemoryExecutor) Propose(ctx context.Context, proposal *rsmapiv1.ProposalInput, stream streams.WriteStream[*rsmapiv1.ProposalOutput]) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sm.Propose(proposal, stream)
	return nil
}

func (e *podMemoryExecutor) Query(ctx context.Context, query *rsmapiv1.QueryInput, stream streams.WriteStream[*rsmapiv1.QueryOutput]) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	e.sm.Query(query, stream)
	return nil
}
