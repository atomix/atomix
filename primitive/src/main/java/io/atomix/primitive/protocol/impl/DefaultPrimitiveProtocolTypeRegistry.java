// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.impl;

import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Primitive protocol type registry.
 */
public class DefaultPrimitiveProtocolTypeRegistry implements PrimitiveProtocolTypeRegistry {
  private final Map<String, PrimitiveProtocol.Type> protocolTypes = new ConcurrentHashMap<>();

  public DefaultPrimitiveProtocolTypeRegistry(Collection<PrimitiveProtocol.Type> protocolTypes) {
    protocolTypes.forEach(type -> this.protocolTypes.put(type.name(), type));
  }

  @Override
  public Collection<PrimitiveProtocol.Type> getProtocolTypes() {
    return protocolTypes.values();
  }

  @Override
  public PrimitiveProtocol.Type getProtocolType(String type) {
    return protocolTypes.get(type);
  }
}
