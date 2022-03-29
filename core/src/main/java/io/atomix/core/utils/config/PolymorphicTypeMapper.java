// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.utils.config;

import io.atomix.core.AtomixRegistry;
import io.atomix.utils.ConfiguredType;
import io.atomix.utils.config.TypedConfig;

/**
 * Polymorphic type mapper.
 */
public class PolymorphicTypeMapper {
  private final String typePath;
  private final Class<? extends TypedConfig> configClass;
  private final Class<? extends ConfiguredType> typeClass;

  public PolymorphicTypeMapper(String typePath, Class<? extends TypedConfig> configClass, Class<? extends ConfiguredType> typeClass) {
    this.typePath = typePath;
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
    return typePath;
  }

  /**
   * Returns the concrete configuration class.
   *
   * @param registry the Atomix type registry
   * @param typeName the type name
   * @return the concrete configuration class
   */
  @SuppressWarnings("unchecked")
  public Class<? extends TypedConfig<?>> getConcreteClass(AtomixRegistry registry, String typeName) {
    ConfiguredType type = registry.getType(typeClass, typeName);
    if (type == null) {
      return null;
    }
    return (Class<? extends TypedConfig<?>>) type.newConfig().getClass();
  }
}
