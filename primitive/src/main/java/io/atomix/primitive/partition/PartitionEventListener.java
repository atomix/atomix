// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.utils.event.EventListener;

/**
 * Partition event listener.
 */
public interface PartitionEventListener extends EventListener<PartitionEvent> {
}
