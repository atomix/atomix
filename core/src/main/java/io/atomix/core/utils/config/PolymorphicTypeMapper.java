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
package io.atomix.core.utils.config;

import io.atomix.core.AtomixRegistry;
import io.atomix.utils.ConfiguredType;
import io.atomix.utils.config.TypedConfig;

/**
 * Polymorphic type mapper.
 */
public class PolymorphicTypeMapper {
  private final Class<? extends TypedConfig> configClass;
  private final Class<? extends ConfiguredType> typeClass;

  public PolymorphicTypeMapper(Class<? extends TypedConfig> configClass, Class<? extends ConfiguredType> typeClass) {
    this.configClass = configClass;
    this.typeClass = typeClass;
  }

  /**
   * Returns the polymorphic configuration class.
   *
   * @return the polymorphic configuration class
   */
  public Class<? extends TypedConfig> getConfigClass() {
    return configClass;
  }

  /**
   * Returns the polymorphic type.
   *
   * @return the polymorphic type
   */
  public Class<? extends ConfiguredType> getTypeClass() {
    return typeClass;
  }

  /**
   * Returns the type path.
   *
   * @return the type path
   */
  public String getTypePath() {
    return "type";
  }

  /**
   * Returns the concrete configuration class.
   *
   * @param registry the Atomix type registry
   * @param type     the type name
   * @return the concrete configuration class
   */
  @SuppressWarnings("unchecked")
  public Class<? extends TypedConfig<?>> getConcreteClass(AtomixRegistry registry, String type) {
    return (Class<? extends TypedConfig<?>>) registry.getType(typeClass, type).newConfig().getClass();
  }
}
