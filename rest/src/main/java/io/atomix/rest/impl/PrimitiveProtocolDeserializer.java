// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.impl;

import io.atomix.core.AtomixRegistry;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;

/**
 * Primitive protocol deserializer.
 */
public class PrimitiveProtocolDeserializer extends PolymorphicTypeDeserializer<PrimitiveProtocolConfig> {
  @SuppressWarnings("unchecked")
  public PrimitiveProtocolDeserializer(AtomixRegistry registry) {
    super(PrimitiveProtocolConfig.class, type -> (Class<? extends PrimitiveProtocolConfig<?>>) registry.getType(PrimitiveProtocol.Type.class, type).newConfig().getClass());
  }
}
