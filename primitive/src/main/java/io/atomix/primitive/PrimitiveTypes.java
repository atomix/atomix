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

import io.atomix.utils.config.ConfigurationException;
import io.atomix.utils.Services;

import java.util.Collection;

/**
 * Primitive types.
 */
public class PrimitiveTypes {

  /**
   * Loads all registered primitive types.
   *
   * @return a collection of all registered primitive types
   */
  public static Collection<PrimitiveType> getPrimitiveTypes() {
    return Services.loadAll(PrimitiveType.class);
  }

  /**
   * Returns the primitive type for the given type.
   *
   * @param typeName the type name for which to return the primitive type
   * @return the primitive type for the given type
   */
  public static PrimitiveType getPrimitiveType(String typeName) {
    for (PrimitiveType type : Services.loadAll(PrimitiveType.class)) {
      if (type.id().replace("_", "-").equalsIgnoreCase(typeName.replace("_", "-"))) {
        return type;
      }
    }
    throw new ConfigurationException("Unknown primitive type: " + typeName);
  }

  private PrimitiveTypes() {
  }
}
