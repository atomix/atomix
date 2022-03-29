// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multiset;

import io.atomix.core.collection.DistributedCollectionConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Distributed multiset configuration.
 */
public class DistributedMultisetConfig extends DistributedCollectionConfig<DistributedMultisetConfig> {
  @Override
  public PrimitiveType getType() {
    return DistributedMultisetType.instance();
  }
}
