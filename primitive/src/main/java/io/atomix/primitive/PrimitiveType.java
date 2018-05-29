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

import io.atomix.primitive.resource.PrimitiveResource;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.AbstractNamed;
import io.atomix.utils.Type;
import io.atomix.utils.config.ConfigurationException;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerConfig;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primitive type.
 */
public class PrimitiveType<
    B extends DistributedPrimitiveBuilder,
    C extends PrimitiveConfig,
    P extends DistributedPrimitive>
    extends AbstractNamed implements Type {

  /**
   * Returns a new primitive type builder.
   *
   * @return a new primitive type builder
   */
  public static Builder builder() {
    return new Builder(null);
  }

  /**
   * Returns a new primitive type builder.
   *
   * @param name the primitive type name
   * @return a new primitive type builder
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  private Class<B> builderClass;
  private Class<C> configClass;
  private Class<P> primitiveClass;
  private Class<? extends PrimitiveService> serviceClass;
  private Class<? extends ServiceConfig> serviceConfigClass = ServiceConfig.class;
  private Class<? extends PrimitiveResource> resourceClass;
  private SerializerConfig namespace;
  private transient volatile Serializer serializer;

  public PrimitiveType() {
    this(null, null, null, null, null, null, null);
  }

  private PrimitiveType(
      String name,
      Class<B> builderClass,
      Class<C> configClass,
      Class<P> primitiveClass,
      Class<? extends PrimitiveService> serviceClass,
      Class<? extends ServiceConfig> serviceConfigClass,
      Class<? extends PrimitiveResource> resourceClass) {
    super(name);
    this.builderClass = builderClass;
    this.configClass = configClass;
    this.primitiveClass = primitiveClass;
    this.serviceClass = serviceClass;
    this.serviceConfigClass = serviceConfigClass != null ? serviceConfigClass : ServiceConfig.class;
    this.resourceClass = resourceClass;
  }

  /**
   * Returns the primitive builder class.
   *
   * @return the primitive builder class
   */
  public Class<B> builderClass() {
    return builderClass;
  }

  /**
   * Returns the primitive configuration class.
   *
   * @return the primitive configuration class
   */
  public Class<C> configClass() {
    return configClass;
  }

  /**
   * Returns the primitive class.
   *
   * @return the primitive class
   */
  public Class<P> primitiveClass() {
    return primitiveClass;
  }

  /**
   * Returns the primitive service class.
   *
   * @return the primitive service class
   */
  public Class<? extends PrimitiveService> serviceClass() {
    return serviceClass;
  }

  /**
   * Returns the service configuration class.
   *
   * @return the service configuration class
   */
  public Class<? extends ServiceConfig> serviceConfigClass() {
    return serviceConfigClass;
  }

  /**
   * Returns the primitive resource class.
   *
   * @return the primitive resource class
   */
  public Class<? extends PrimitiveResource> resourceClass() {
    return resourceClass;
  }

  /**
   * Returns the primitive type serializer.
   *
   * @return the primitive type serializer
   */
  public Serializer serializer() {
    if (serializer == null) {
      synchronized (this) {
        if (serializer == null) {
          if (namespace != null) {
            serializer = Serializer.using(KryoNamespace.builder()
                .register(KryoNamespaces.BASIC)
                .register(serviceConfigClass())
                .register(new KryoNamespace(namespace))
                .build());
          } else {
            serializer = Serializer.using(KryoNamespace.builder()
                .register(KryoNamespaces.BASIC)
                .register(serviceConfigClass())
                .build());
          }
        }
      }
    }
    return serializer;
  }

  /**
   * Returns a new configuration for the primitive type.
   *
   * @return a new primitive configuration
   */
  public C newConfig() {
    if (configClass == null) {
      throw new ConfigurationException("No configuration class specified for primitive type " + name());
    }

    try {
      return configClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to instantiate configuration for primitive type " + name() + ". Must provide a no-arg constructor", e);
    }
  }

  /**
   * Returns a new primitive builder.
   *
   * @param primitiveName     the primitive name
   * @param managementService the primitive management service
   * @return a new primitive builder
   */
  public B newBuilder(String primitiveName, PrimitiveManagementService managementService) {
    return newBuilder(primitiveName, newConfig(), managementService);
  }

  /**
   * Returns a new primitive builder.
   *
   * @param primitiveName     the primitive name
   * @param config            the primitive configuration
   * @param managementService the primitive management service
   * @return a new primitive builder
   */
  public B newBuilder(String primitiveName, C config, PrimitiveManagementService managementService) {
    if (builderClass == null) {
      throw new ConfigurationException("No builder class configured for primitive type " + name());
    }

    try {
      Constructor<? extends B> constructor = builderClass.getConstructor(PrimitiveType.class, String.class, configClass, PrimitiveManagementService.class);
      return constructor.newInstance(this, primitiveName, config, managementService);
    } catch (NoSuchMethodException e) {
      throw new ConfigurationException("No valid constructor found for builder class " + builderClass.getName(), e);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new ConfigurationException("Failed to instantiate primitive builder class " + builderClass.getName(), e);
    }
  }

  /**
   * Creates a new service instance from the given configuration.
   *
   * @param config the service configuration
   * @return the service instance
   */
  public PrimitiveService newService(ServiceConfig config) {
    if (serviceClass == null) {
      throw new ConfigurationException("No service class configured");
    }

    try {
      Constructor<? extends PrimitiveService> configConstructor = serviceClass.getConstructor(serviceConfigClass);
      try {
        return configConstructor.newInstance(config);
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new ConfigurationException("Failed to instantiate primitive service using config constructor", e);
      }
    } catch (NoSuchMethodException e) {
      try {
        Constructor<? extends PrimitiveService> noArgConstructor = serviceClass.getConstructor();
        try {
          return noArgConstructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e2) {
          throw new ConfigurationException("Failed to instantiate primitive service using no-arg constructor", e2);
        }
      } catch (NoSuchMethodException e2) {
        throw new ConfigurationException("Failed to instantiate primitive service: no valid constructor found", e);
      }
    }
  }

  /**
   * Creates a new resource for the given primitive.
   *
   * @param primitive the primitive instance
   * @return a new resource for the given primitive instance
   */
  public PrimitiveResource newResource(P primitive) {
    if (resourceClass == null) {
      throw new ConfigurationException("No resource class configured for primitive type " + name());
    }

    try {
      return resourceClass.getConstructor(primitiveClass).newInstance(primitive);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new ConfigurationException("Failed to instantiate resource class for primitive type " + name(), e);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(name());
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof PrimitiveType && Objects.equals(((PrimitiveType) object).name(), name());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }

  /**
   * Primitive type builder.
   */
  public static class Builder implements io.atomix.utils.Builder<PrimitiveType> {
    private String name;
    private Class<? extends DistributedPrimitiveBuilder> builderClass;
    private Class<? extends PrimitiveConfig> configClass;
    private Class<? extends DistributedPrimitive> primitiveClass;
    private Class<? extends PrimitiveService> serviceClass;
    private Class<? extends ServiceConfig> serviceConfigClass = ServiceConfig.class;
    private Class<? extends PrimitiveResource> resourceClass;

    private Builder(String name) {
      this.name = name;
    }

    /**
     * Sets the primitive type name.
     *
     * @param name the primitive type name
     * @return the primitive type builder
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the primitive builder class.
     *
     * @param builderClass the primitive builder class
     * @return the primitive type builder
     */
    public Builder withBuilderClass(Class<? extends DistributedPrimitiveBuilder> builderClass) {
      this.builderClass = builderClass;
      return this;
    }

    /**
     * Sets the primitive configuration class.
     *
     * @param configClass the primitive configuration class
     * @return the primitive type builder
     */
    public Builder withConfigClass(Class<? extends PrimitiveConfig> configClass) {
      this.configClass = configClass;
      return this;
    }

    /**
     * Sets the primitive class.
     *
     * @param primitiveClass the builder class
     * @return the primitive type builder
     */
    public Builder withPrimitiveClass(Class<? extends DistributedPrimitive> primitiveClass) {
      this.primitiveClass = primitiveClass;
      return this;
    }

    /**
     * Sets the primitive service class.
     *
     * @param serviceClass the primitive service class
     * @return the primitive type builder
     */
    public Builder withServiceClass(Class<? extends PrimitiveService> serviceClass) {
      this.serviceClass = serviceClass;
      return this;
    }

    /**
     * Sets the primitive service configuration class.
     *
     * @param serviceConfigClass the primitive service configuration class
     * @return the primitive type builder
     */
    public Builder withServiceConfigClass(Class<? extends ServiceConfig> serviceConfigClass) {
      this.serviceConfigClass = serviceConfigClass;
      return this;
    }

    /**
     * Sets the primitive resource class.
     *
     * @param resourceClass the primitive resource class
     * @return the primitive type builder
     */
    public Builder withResourceClass(Class<? extends PrimitiveResource> resourceClass) {
      this.resourceClass = resourceClass;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public PrimitiveType build() {
      return new PrimitiveType(name, builderClass, configClass, primitiveClass, serviceClass, serviceConfigClass, resourceClass);
    }
  }
}
