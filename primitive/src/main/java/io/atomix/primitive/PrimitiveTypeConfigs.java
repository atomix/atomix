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
package io.atomix.primitive;

import io.atomix.utils.Config;

import java.util.HashMap;
import java.util.Map;

/**
 * Primitive type configuration.
 */
public class PrimitiveTypeConfigs implements Config {
  private Map<String, Class<? extends PrimitiveType>> types = new HashMap<>();

  /**
   * Returns the primitive types.
   *
   * @return the primitive types
   */
  public Map<String, Class<? extends PrimitiveType>> getTypes() {
    return types;
  }

  /**
   * Sets the primitive types.
   *
   * @param types the primitive types
   * @return the primitive type configuration
   */
  public PrimitiveTypeConfigs setTypes(Map<String, Class<? extends PrimitiveType>> types) {
    this.types = types;
    return this;
  }

  /**
   * Adds a primitive type.
   *
   * @param name the type name
   * @param type the type class
   * @return the primitive type configuration
   */
  public PrimitiveTypeConfigs addType(String name, Class<? extends PrimitiveType> type) {
    types.put(name, type);
    return this;
  }
}
