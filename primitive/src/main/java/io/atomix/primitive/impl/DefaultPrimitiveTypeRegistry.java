// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.impl;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.utils.ServiceException;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Primitive type registry.
 */
public class DefaultPrimitiveTypeRegistry implements PrimitiveTypeRegistry {
  private final Map<String, PrimitiveType> primitiveTypes = new ConcurrentHashMap<>();

  public DefaultPrimitiveTypeRegistry(Collection<PrimitiveType> primitiveTypes) {
    primitiveTypes.forEach(type -> this.primitiveTypes.put(type.name(), type));
  }

  @Override
  public Collection<PrimitiveType> getPrimitiveTypes() {
    return primitiveTypes.values();
  }

  @Override
  public PrimitiveType getPrimitiveType(String typeName) {
    PrimitiveType type = primitiveTypes.get(typeName);
    if (type == null) {
      throw new ServiceException("Unknown primitive type " + typeName);
    }
    return type;
  }
}
