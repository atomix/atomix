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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primitive configurations.
 */
public class PrimitiveConfigs implements Config {
  private Map<String, PrimitiveConfig> primitives = new HashMap<>();

  /**
   * Returns the primitive configurations.
   *
   * @return the primitive configurations
   */
  public Map<String, PrimitiveConfig> getPrimitives() {
    return primitives;
  }

  /**
   * Sets the primitive configurations.
   *
   * @param primitives the primitive configurations
   * @return the primitive configuration holder
   */
  public PrimitiveConfigs setPrimitives(Map<String, PrimitiveConfig> primitives) {
    this.primitives = checkNotNull(primitives);
    return this;
  }

  /**
   * Adds a primitive configuration.
   *
   * @param name the primitive name
   * @param config the primitive configuration
   * @return the primitive configuration holder
   */
  public PrimitiveConfigs addPrimitive(String name, PrimitiveConfig config) {
    primitives.put(name, config);
    return this;
  }

  /**
   * Returns a primitive configuration.
   *
   * @param name the primitive name
   * @param <C> the configuration type
   * @return the primitive configuration
   */
  public <C extends PrimitiveConfig<C>> C getPrimitive(String name) {
    return (C) primitives.get(name);
  }
}
