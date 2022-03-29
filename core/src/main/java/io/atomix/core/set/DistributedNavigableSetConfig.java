// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set;

import io.atomix.core.collection.DistributedCollectionConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Distributed navigable set configuration.
 */
public class DistributedNavigableSetConfig extends DistributedCollectionConfig<DistributedNavigableSetConfig> {
  @Override
  public PrimitiveType getType() {
    return DistributedNavigableSetType.instance();
  }
}
