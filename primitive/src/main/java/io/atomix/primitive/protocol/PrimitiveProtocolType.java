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
package io.atomix.primitive.protocol;

import io.atomix.utils.AbstractNamed;
import io.atomix.utils.Type;
import io.atomix.utils.config.Config;
import io.atomix.utils.config.ConfigurationException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed primitive protocol type.
 */
public class PrimitiveProtocolType extends AbstractNamed implements Type, Config {

  /**
   * Returns a new protocol type builder.
   *
   * @return the protocol type builder
   */
  public static Builder builder() {
    return new Builder(null);
  }

  /**
   * Returns a new protocol type builder.
   *
   * @param name the protocol type name
   * @return the protocol type builder
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  private Class<? extends PrimitiveProtocolConfig> configClass;
  private Class<? extends PrimitiveProtocolFactory> factoryClass;
  private Class<? extends PrimitiveProtocol> protocolClass;

  public PrimitiveProtocolType() {
    this(null, null, null, null);
  }

  private PrimitiveProtocolType(
      String name,
      Class<? extends PrimitiveProtocolConfig> configClass,
      Class<? extends PrimitiveProtocolFactory> factoryClass,
      Class<? extends PrimitiveProtocol> protocolClass) {
    super(name);
    this.configClass = configClass;
    this.factoryClass = factoryClass;
    this.protocolClass = protocolClass;
  }

  /**
   * Returns the protocol configuration class.
   *
   * @return the protocol configuration class
   */
  public Class<? extends PrimitiveProtocolConfig> configClass() {
    return configClass;
  }

  /**
   * Returns the protocol factory class.
   *
   * @return the protocol factory class
   */
  public Class<? extends PrimitiveProtocolFactory> factoryClass() {
    return factoryClass;
  }

  /**
   * Returns the protocol class.
   *
   * @return the protocol class
   */
  public Class<? extends PrimitiveProtocol> protocolClass() {
    return protocolClass;
  }

  /**
   * Creates a new protocol instance.
   *
   * @param config the protocol configuration
   * @return the protocol instance
   */
  @SuppressWarnings("unchecked")
  public PrimitiveProtocol createProtocol(PrimitiveProtocolConfig config) {
    if (factoryClass != null) {
      try {
        return factoryClass.newInstance().createProtocol(config);
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ConfigurationException("Failed to instantiate protocol factory", e);
      }
    }

    if (protocolClass != null) {
      try {
        Constructor<? extends PrimitiveProtocol> constructor = protocolClass.getConstructor(configClass);
        return constructor.newInstance(config);
      } catch (NoSuchMethodException e) {
        try {
          Constructor<? extends PrimitiveProtocol> constructor = protocolClass.getConstructor(PrimitiveProtocolConfig.class);
          return constructor.newInstance(config);
        } catch (NoSuchMethodException e2) {
          throw new ConfigurationException("No configuration constructor found for protocol class " + protocolClass.getName());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e2) {
          throw new ConfigurationException("Failed to instantiate primitive protocol", e2);
        }
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new ConfigurationException("Failed to instantiate primitive protocol", e);
      }
    }
    throw new ConfigurationException("No protocol factory specified");
  }

  @Override
  public int hashCode() {
    return Objects.hash(name());
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof PrimitiveProtocolType && Objects.equals(((PrimitiveProtocolType) object).name(), name());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }

  /**
   * Primitive protocol type builder.
   */
  public static class Builder implements io.atomix.utils.Builder<PrimitiveProtocolType> {
    private String name;
    private Class<? extends PrimitiveProtocolConfig> configClass;
    private Class<? extends PrimitiveProtocolFactory> factoryClass;
    private Class<? extends PrimitiveProtocol> protocolClass;

    private Builder(String name) {
      this.name = name;
    }

    /**
     * Sets the protocol type name.
     *
     * @param name the protocol type name
     * @return the protocol type builder
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the protocol configuration class.
     *
     * @param configClass the protocol configuration class
     * @return the protocol type builder
     */
    public Builder withConfigClass(Class<? extends PrimitiveProtocolConfig> configClass) {
      this.configClass = configClass;
      return this;
    }

    /**
     * Sets the protocol factory class.
     *
     * @param factoryClass the protocol factory class
     * @return the protocol type builder
     */
    public Builder withFactoryClass(Class<? extends PrimitiveProtocolFactory> factoryClass) {
      this.factoryClass = factoryClass;
      return this;
    }

    /**
     * Sets the protocol class.
     *
     * @param protocolClass the protocol class
     * @return the protocol type builder
     */
    public Builder withProtocolClass(Class<? extends PrimitiveProtocol> protocolClass) {
      this.protocolClass = protocolClass;
      return this;
    }

    @Override
    public PrimitiveProtocolType build() {
      return new PrimitiveProtocolType(name, configClass, factoryClass, protocolClass);
    }
  }
}
