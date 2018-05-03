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

import io.atomix.primitive.resource.PrimitiveResource;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.Generics;
import io.atomix.utils.Identifier;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Namespace;

/**
 * Raft service type.
 */
public interface PrimitiveType<B extends DistributedPrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends DistributedPrimitive, S extends ServiceConfig>
    extends Identifier<String> {

  /**
   * Returns the primitive type name.
   *
   * @return the primitive type name
   */
  @Override
  String id();

  /**
   * Returns the primitive type builder class.
   *
   * @return the primitive type builder class
   */
  @SuppressWarnings("unchecked")
  default Class<? extends DistributedPrimitiveBuilder> builderClass() {
    return (Class<? extends DistributedPrimitiveBuilder>) Generics.getGenericInterfaceType(this, PrimitiveType.class, 0);
  }

  /**
   * Returns the primitive type configuration class.
   *
   * @return the primitive type configuration class
   */
  @SuppressWarnings("unchecked")
  default Class<? extends PrimitiveConfig> primitiveConfigClass() {
    return (Class<? extends PrimitiveConfig>) Generics.getGenericInterfaceType(this, PrimitiveType.class, 1);
  }

  /**
   * Returns the primitive class.
   *
   * @return the primitive class
   */
  @SuppressWarnings("unchecked")
  default Class<? extends DistributedPrimitive> primitiveClass() {
    return (Class<? extends DistributedPrimitive>) Generics.getGenericInterfaceType(this, PrimitiveType.class, 2);
  }

  /**
   * Returns the service configuration class.
   *
   * @return the service configuration class
   */
  @SuppressWarnings("unchecked")
  default Class<? extends ServiceConfig> serviceConfigClass() {
    Class<? extends ServiceConfig> serviceConfigClass = (Class<? extends ServiceConfig>) Generics.getGenericInterfaceType(this, ServiceConfig.class, 3);
    return serviceConfigClass != null ? serviceConfigClass : ServiceConfig.class;
  }

  /**
   * Returns the primitive namespace.
   *
   * @return the primitive namespace
   */
  default Namespace namespace() {
    return KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(serviceConfigClass())
        .build();
  }

  /**
   * Returns the primitive service factory.
   *
   * @param config the primitive service configuration
   * @return the primitive service factory.
   */
  PrimitiveService newService(S config);

  /**
   * Returns the primitive resource factory.
   *
   * @param primitive the primitive instance
   * @return the primitive resource factory
   */
  default PrimitiveResource newResource(P primitive) {
    return null;
  }

  /**
   * Returns a new primitive builder for the given partition.
   *
   * @param name              the primitive name
   * @param managementService the primitive management service
   * @return the primitive builder
   */
  B newPrimitiveBuilder(String name, PrimitiveManagementService managementService);

  /**
   * Returns a new primitive builder for the given partition.
   *
   * @param name              the primitive name
   * @param config            the primitive configuration
   * @param managementService the primitive management service
   * @return the primitive builder
   */
  B newPrimitiveBuilder(String name, C config, PrimitiveManagementService managementService);

}
