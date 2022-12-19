// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"hash/fnv"
	"sync"
)

func NewProtocol() *Protocol {
	return &Protocol{}
}

type Protocol struct {
	partitions []*PartitionClient
	mu         sync.RWMutex
}

func (p *Protocol) PartitionIndex(partitionKey []byte) int {
	i, err := getPartitionIndex(partitionKey, len(p.partitions))
	if err != nil {
		panic(err)
	}
	return i
}

func (p *Protocol) PartitionBy(partitionKey []byte) *PartitionClient {
	i, err := getPartitionIndex(partitionKey, len(p.partitions))
	if err != nil {
		panic(err)
	}
	return p.partitions[i]
}

func (p *Protocol) Partitions() []*PartitionClient {
	return p.partitions
}

// getPartitionIndex returns the index of the partition for the given key
func getPartitionIndex(key []byte, partitions int) (int, error) {
	h := fnv.New32a()
	if _, err := h.Write(key); err != nil {
		return 0, err
	}
	return int(h.Sum32() % uint32(partitions)), nil
}
