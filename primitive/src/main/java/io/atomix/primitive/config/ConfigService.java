// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.config;

import io.atomix.primitive.PrimitiveType;

/**
 * Configuration service.
 */
public interface ConfigService {

  /**
   * Returns the registered configuration for the given primitive.
   *
   * @param primitiveName the primitive name
   * @param primitiveType the primitive type
   * @param <C>           the configuration type
   * @return the primitive configuration
   */
  <C extends PrimitiveConfig<C>> C getConfig(String primitiveName, PrimitiveType primitiveType);

}
