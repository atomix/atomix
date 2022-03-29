// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.impl;

import io.atomix.core.AtomixRegistry;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;

/**
 * Partition group deserializer.
 */
public class PartitionGroupDeserializer extends PolymorphicTypeDeserializer<PartitionGroupConfig> {
  @SuppressWarnings("unchecked")
  public PartitionGroupDeserializer(AtomixRegistry registry) {
    super(PartitionGroup.class, type -> (Class<? extends PartitionGroupConfig<?>>) registry.getType(PartitionGroup.Type.class, type).newConfig().getClass());
  }
}
