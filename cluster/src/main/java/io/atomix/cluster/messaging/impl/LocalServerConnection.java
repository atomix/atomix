// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import java.util.Optional;

/**
 * Local server-side connection.
 */
final class LocalServerConnection extends AbstractServerConnection {
  private static final byte[] EMPTY_PAYLOAD = new byte[0];

  private volatile LocalClientConnection clientConnection;

  LocalServerConnection(HandlerRegistry handlers, LocalClientConnection clientConnection) {
    super(handlers);
    this.clientConnection = clientConnection;
  }

  @Override
  public void reply(ProtocolRequest message, ProtocolReply.Status status, Optional<byte[]> payload) {
    LocalClientConnection clientConnection = this.clientConnection;
    if (clientConnection != null) {
      clientConnection.dispatch(new ProtocolReply(message.id(), payload.orElse(EMPTY_PAYLOAD), status));
    }
  }
}
