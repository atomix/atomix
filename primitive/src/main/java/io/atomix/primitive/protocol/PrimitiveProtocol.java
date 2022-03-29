// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol;

import io.atomix.utils.ConfiguredType;

/**
 * Primitive protocol.
 */
public interface PrimitiveProtocol {

  /**
   * Distributed primitive protocol type.
   */
  interface Type<C extends PrimitiveProtocolConfig<C>> extends ConfiguredType<C>, Comparable<Type<C>> {

    /**
     * Creates a new protocol instance.
     *
     * @param config the protocol configuration
     * @return the protocol instance
     */
    PrimitiveProtocol newProtocol(C config);

    @Override
    default int compareTo(Type<C> o) {
      return name().compareTo(o.name());
    }
  }

  /**
   * Returns the protocol type.
   *
   * @return the protocol type
   */
  Type type();

}
