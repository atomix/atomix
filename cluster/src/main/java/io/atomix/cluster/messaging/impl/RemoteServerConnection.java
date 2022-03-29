// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import io.netty.channel.Channel;

import java.util.Optional;

/**
 * Remote server connection manages messaging on the server side of a Netty connection.
 */
final class RemoteServerConnection extends AbstractServerConnection {
  private static final byte[] EMPTY_PAYLOAD = new byte[0];

  private final Channel channel;

  RemoteServerConnection(HandlerRegistry handlers, Channel channel) {
    super(handlers);
    this.channel = channel;
  }

  @Override
  public void reply(ProtocolRequest message, ProtocolReply.Status status, Optional<byte[]> payload) {
    ProtocolReply response = new ProtocolReply(
        message.id(),
        payload.orElse(EMPTY_PAYLOAD),
        status);
    channel.writeAndFlush(response, channel.voidPromise());
  }
}
