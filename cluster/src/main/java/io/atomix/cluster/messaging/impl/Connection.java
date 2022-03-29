// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

interface Connection<M extends ProtocolMessage> {
  /**
   * Dispatches a message received on the connection.
   *
   * @param message the message to dispatch
   */
  void dispatch(M message);

  /**
   * Closes the connection.
   */
  default void close() {
  }
}
