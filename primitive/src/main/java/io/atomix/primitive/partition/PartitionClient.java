// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.service.ServiceConfig;

/**
 * Primitive client.
 */
public interface PartitionClient {

  /**
   * Returns a new session builder for the given primitive type.
   *
   * @param primitiveName the proxy name
   * @param primitiveType the type for which to return a new proxy builder
   * @param serviceConfig the primitive service configuration
   * @return a new proxy builder for the given primitive type
   */
  SessionClient.Builder sessionBuilder(String primitiveName, PrimitiveType primitiveType, ServiceConfig serviceConfig);

}
