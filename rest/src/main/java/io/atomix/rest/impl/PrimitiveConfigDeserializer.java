// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.impl;

import io.atomix.core.AtomixRegistry;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.PrimitiveConfig;

/**
 * Primitive configuration deserializer.
 */
public class PrimitiveConfigDeserializer extends PolymorphicTypeDeserializer<PrimitiveConfig> {
  @SuppressWarnings("unchecked")
  public PrimitiveConfigDeserializer(AtomixRegistry registry) {
    super(PrimitiveConfig.class, type -> registry.getType(PrimitiveType.class, type).newConfig().getClass());
  }
}
