// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	rsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"sync"
)

func NewProtocol(registry *statemachine.PrimitiveTypeRegistry) node.Protocol {
	return &memoryProtocol{
		partitions: map[rsmv1.PartitionID]node.Partition{
			1: node.NewPartition(1, newExecutor(registry)),
			2: node.NewPartition(2, newExecutor(registry)),
			3: node.NewPartition(3, newExecutor(registry)),
		},
	}
}

type memoryProtocol struct {
	partitions map[rsmv1.PartitionID]node.Partition
}

func (p *memoryProtocol) Partitions() []node.Partition {
	partitions := make([]node.Partition, 0, len(p.partitions))
	for _, partition := range p.partitions {
		partitions = append(partitions, partition)
	}
	return partitions
}

func (p *memoryProtocol) Partition(partitionID rsmv1.PartitionID) (node.Partition, bool) {
	partition, ok := p.partitions[partitionID]
	return partition, ok
}

func newExecutor(registry *statemachine.PrimitiveTypeRegistry) node.Executor {
	return &memoryExecutor{
		sm: statemachine.NewStateMachine(registry),
	}
}

type memoryExecutor struct {
	sm statemachine.StateMachine
	mu sync.RWMutex
}

func (e *memoryExecutor) Propose(ctx context.Context, proposal *rsmv1.ProposalInput, stream streams.WriteStream[*rsmv1.ProposalOutput]) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sm.Propose(proposal, stream)
	return nil
}

func (e *memoryExecutor) Query(ctx context.Context, query *rsmv1.QueryInput, stream streams.WriteStream[*rsmv1.QueryOutput]) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	e.sm.Query(query, stream)
	return nil
}
