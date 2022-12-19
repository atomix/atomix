// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import protocol "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"

type Protocol interface {
	Partitions() []Partition
	Partition(partitionID protocol.PartitionID) (Partition, bool)
}
