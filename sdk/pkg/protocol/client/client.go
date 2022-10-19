// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/network"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"sort"
)

var log = logging.GetLogger()

func NewClient(network network.Network) *ProtocolClient {
	return &ProtocolClient{
		Protocol: NewProtocol(),
		network:  network,
	}
}

type ProtocolClient struct {
	*Protocol
	config  protocol.ProtocolConfig
	network network.Network
}

func (c *ProtocolClient) Connect(ctx context.Context, config protocol.ProtocolConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.partitions) > 0 {
		return errors.NewConflict("client already connected")
	}

	sessionTimeout := defaultSessionTimeout
	if config.SessionTimeout != nil {
		sessionTimeout = *config.SessionTimeout
	}

	for _, partitionConfig := range config.Partitions {
		partition := newPartition(partitionConfig.PartitionID, c.network, sessionTimeout)
		if err := partition.connect(ctx, &partitionConfig); err != nil {
			return err
		}
		c.partitions = append(c.partitions, partition)
	}

	sort.Slice(c.partitions, func(i, j int) bool {
		return c.partitions[i].id < c.partitions[j].id
	})

	c.config = config
	return nil
}

func (c *ProtocolClient) Configure(ctx context.Context, config protocol.ProtocolConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = config

	partitions := make(map[protocol.PartitionID]*PartitionClient)
	for _, partition := range c.partitions {
		partitions[partition.id] = partition
	}
	for _, partitionConfig := range config.Partitions {
		partition, ok := partitions[partitionConfig.PartitionID]
		if !ok {
			return errors.NewInternal("partition %d not found", partitionConfig.PartitionID)
		}
		if err := partition.configure(&partitionConfig); err != nil {
			return err
		}
	}
	return nil
}

func (c *ProtocolClient) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, partition := range c.partitions {
		if err := partition.close(ctx); err != nil {
			return err
		}
	}
	return nil
}
