// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.queue;

import io.atomix.core.collection.DistributedCollectionConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Distributed queue configuration.
 */
public class DistributedQueueConfig extends DistributedCollectionConfig<DistributedQueueConfig> {
  @Override
  public PrimitiveType getType() {
    return DistributedQueueType.instance();
  }
}
