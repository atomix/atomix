// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.set;

/**
 * Set builder.
 */
public interface SetCompatibleBuilder<B extends SetCompatibleBuilder<B>> {

  /**
   * Configures the builder with a set compatible gossip protocol.
   *
   * @param protocol the set protocol
   * @return the primitive builder
   */
  B withProtocol(SetProtocol protocol);

}
