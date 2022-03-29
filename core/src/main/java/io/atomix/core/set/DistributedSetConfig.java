// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set;

import io.atomix.core.collection.DistributedCollectionConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Distributed set configuration.
 */
public class DistributedSetConfig extends DistributedCollectionConfig<DistributedSetConfig> {
  @Override
  public PrimitiveType getType() {
    return DistributedSetType.instance();
  }
}
