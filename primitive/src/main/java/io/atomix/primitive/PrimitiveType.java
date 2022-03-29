// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.ConfiguredType;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

/**
 * Primitive type.
 */
public interface PrimitiveType<B extends PrimitiveBuilder, C extends PrimitiveConfig, P extends SyncPrimitive> extends ConfiguredType<C> {

  /**
   * Returns the primitive type namespace.
   *
   * @return the primitive type namespace
   */
  default Namespace namespace() {
    return Namespace.builder()
        .register(Namespaces.BASIC)
        .register(ServiceConfig.class)
        .build();
  }

  /**
   * Returns a new instance of the primitive configuration.
   *
   * @return a new instance of the primitive configuration
   */
  @Override
  C newConfig();

  /**
   * Returns a new primitive builder.
   *
   * @param primitiveName     the primitive name
   * @param config            the primitive configuration
   * @param managementService the primitive management service
   * @return a new primitive builder
   */
  B newBuilder(String primitiveName, C config, PrimitiveManagementService managementService);

  /**
   * Creates a new service instance from the given configuration.
   *
   * @param config the service configuration
   * @return the service instance
   */
  PrimitiveService newService(ServiceConfig config);
}
