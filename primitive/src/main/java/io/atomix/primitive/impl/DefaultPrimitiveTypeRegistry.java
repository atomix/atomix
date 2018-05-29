/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitive.impl;

import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.utils.config.ConfigurationException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Default primitive type registry.
 */
public class DefaultPrimitiveTypeRegistry implements PrimitiveTypeRegistry {
  private final Map<String, PrimitiveType> types = Maps.newConcurrentMap();

  public DefaultPrimitiveTypeRegistry() {
    this(new ArrayList<>());
  }

  public DefaultPrimitiveTypeRegistry(Collection<PrimitiveType> types) {
    types.forEach(type -> this.types.put(type.name(), type));
  }

  @Override
  public void addPrimitiveType(PrimitiveType type) {
    types.put(type.name(), type);
  }

  @Override
  public void removePrimitiveType(PrimitiveType type) {
    types.remove(type.name());
  }

  @Override
  public Collection<PrimitiveType> getPrimitiveTypes() {
    return types.values();
  }

  @Override
  public PrimitiveType getPrimitiveType(String typeName) {
    PrimitiveType type = types.get(typeName);
    if (type == null) {
      throw new ConfigurationException("Unknown primitive type " + typeName);
    }
    return type;
  }
}
