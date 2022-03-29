// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import java.util.Optional;

/**
 * Server-side connection interface which handles replying to messages.
 */
interface ServerConnection extends Connection<ProtocolRequest> {

  /**
   * Sends a reply to the other side of the connection.
   *
   * @param message the message to which to reply
   * @param status  the reply status
   * @param payload the response payload
   */
  void reply(ProtocolRequest message, ProtocolReply.Status status, Optional<byte[]> payload);

  /**
   * Closes the connection.
   */
  default void close() {
  }
}
