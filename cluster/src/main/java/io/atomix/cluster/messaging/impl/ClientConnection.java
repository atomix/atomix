// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Client-side connection interface which handles sending messages.
 */
interface ClientConnection extends Connection<ProtocolReply> {

  /**
   * Sends a message to the other side of the connection.
   *
   * @param message the message to send
   * @return a completable future to be completed once the message has been sent
   */
  CompletableFuture<Void> sendAsync(ProtocolRequest message);

  /**
   * Sends a message to the other side of the connection, awaiting a reply.
   *
   * @param message the message to send
   * @param timeout the response timeout
   * @return a completable future to be completed once a reply is received or the request times out
   */
  CompletableFuture<byte[]> sendAndReceive(ProtocolRequest message, Duration timeout);

  /**
   * Closes the connection.
   */
  default void close() {
  }
}
