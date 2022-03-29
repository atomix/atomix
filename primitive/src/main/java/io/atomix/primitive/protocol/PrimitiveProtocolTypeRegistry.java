// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol;

import java.util.Collection;

/**
 * Primitive protocol type registry.
 */
public interface PrimitiveProtocolTypeRegistry {

  /**
   * Returns the collection of registered protocol types.
   *
   * @return the collection of registered protocol types
   */
  Collection<PrimitiveProtocol.Type> getProtocolTypes();

  /**
   * Returns the protocol type for the given configuration.
   *
   * @param type the type name for which to return the protocol type
   * @return the protocol type for the given configuration
   */
  PrimitiveProtocol.Type getProtocolType(String type);

}
