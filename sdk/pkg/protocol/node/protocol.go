// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import "github.com/atomix/runtime/sdk/pkg/protocol"

type Protocol interface {
	Partitions() []Partition
	Partition(partitionID protocol.PartitionID) (Partition, bool)
}
