// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol;

/**
 * Log compatible primitive.
 */
public interface LogCompatibleBuilder<B extends LogCompatibleBuilder<B>> {

  /**
   * Configures the builder with a log replication protocol.
   *
   * @param protocol the log protocol
   * @return the primitive builder
   */
  B withProtocol(LogProtocol protocol);

}
