// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set;

import io.atomix.core.collection.DistributedCollectionConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Distributed sorted set configuration.
 */
public class DistributedSortedSetConfig extends DistributedCollectionConfig<DistributedSortedSetConfig> {
  @Override
  public PrimitiveType getType() {
    return DistributedSortedSetType.instance();
  }
}
