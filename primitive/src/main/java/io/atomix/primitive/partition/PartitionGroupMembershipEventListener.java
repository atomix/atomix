// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.utils.event.EventListener;

/**
 * Partition group membership event listener.
 */
public interface PartitionGroupMembershipEventListener extends EventListener<PartitionGroupMembershipEvent> {
}
