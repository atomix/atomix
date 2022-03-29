// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol;

/**
 * State machine replication compatible primitive.
 */
public interface ProxyCompatibleBuilder<B extends ProxyCompatibleBuilder<B>> {

  /**
   * Configures the builder with a state machine replication protocol.
   *
   * @param protocol the state machine replication protocol
   * @return the primitive builder
   */
  B withProtocol(ProxyProtocol protocol);

}
