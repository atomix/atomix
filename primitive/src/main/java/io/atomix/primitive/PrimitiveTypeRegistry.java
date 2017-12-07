/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitive;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Primitive registry.
 */
public class PrimitiveTypeRegistry {
  private final Map<String, PrimitiveType> types = Maps.newConcurrentMap();

  /**
   * Registers a primitive type.
   *
   * @param type the primitive type
   */
  public void register(PrimitiveType type) {
    types.put(type.id(), type);
  }

  /**
   * Unregisters a primitive type.
   *
   * @param type the primitive type
   */
  public void unregister(PrimitiveType type) {
    types.remove(type.id());
  }

  /**
   * Returns a primitive type by name.
   *
   * @param typeName the primitive type name
   * @return the primitive type or {@code null} if no type with the given name is registered
   */
  public PrimitiveType get(String typeName) {
    return types.get(typeName);
  }

  /**
   * Returns the number of registered primitive types.
   *
   * @return the number of registered primitive types
   */
  public int size() {
    return types.size();
  }
}
