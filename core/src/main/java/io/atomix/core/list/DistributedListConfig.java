// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.list;

import io.atomix.core.collection.DistributedCollectionConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Distributed list configuration.
 */
public class DistributedListConfig extends DistributedCollectionConfig<DistributedListConfig> {
  @Override
  public PrimitiveType getType() {
    return DistributedListType.instance();
  }
}
