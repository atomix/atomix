// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/network"
)

var log = logging.GetLogger()

func NewClient(network network.Driver, opts ...Option) *ProtocolClient {
	var options Options
	options.apply(opts...)
	return &ProtocolClient{
		Options:  options,
		Protocol: NewProtocol(),
		network:  network,
	}
}

type ProtocolClient struct {
	Options
	*Protocol
	config  protocol.ProtocolConfig
	network network.Driver
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
		partition := newPartition(partitionConfig.PartitionID, c, sessionTimeout)
		if err := partition.connect(ctx, &partitionConfig); err != nil {
			return err
		}
		c.partitions = append(c.partitions, partition)
	}

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
