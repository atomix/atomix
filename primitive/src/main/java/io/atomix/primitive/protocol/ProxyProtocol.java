// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.service.ServiceConfig;

/**
 * State machine replication-based primitive protocol.
 */
public interface ProxyProtocol extends PrimitiveProtocol {

  /**
   * Returns the protocol partition group name.
   *
   * @return the protocol partition group name
   */
  String group();

  /**
   * Returns a new primitive proxy for the given partition group.
   *
   * @param primitiveName    the primitive name
   * @param primitiveType    the primitive type
   * @param serviceType      the primitive service type
   * @param serviceConfig    the service configuration
   * @param partitionService the partition service
   * @return the proxy for the given partition group
   */
  <S> ProxyClient<S> newProxy(
      String primitiveName,
      PrimitiveType primitiveType,
      Class<S> serviceType,
      ServiceConfig serviceConfig,
      PartitionService partitionService);
}
