// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.value;

/**
 * Value builder.
 */
public interface ValueCompatibleBuilder<B extends ValueCompatibleBuilder<B>> {

  /**
   * Configures the builder with a value compatible gossip protocol.
   *
   * @param protocol the value protocol
   * @return the primitive builder
   */
  B withProtocol(ValueProtocol protocol);

}
