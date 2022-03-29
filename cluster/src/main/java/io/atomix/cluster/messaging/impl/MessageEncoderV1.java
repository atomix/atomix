// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import java.net.InetAddress;

import io.atomix.utils.net.Address;
import io.netty.buffer.ByteBuf;

/**
 * V1 message encoder.
 */
class MessageEncoderV1 extends AbstractMessageEncoder {
  MessageEncoderV1(Address address) {
    super(address);
  }

  @Override
  protected void encodeAddress(ProtocolMessage message, ByteBuf buffer) {
    final InetAddress senderIp = address.address();
    final byte[] senderIpBytes = senderIp.getAddress();
    buffer.writeByte(senderIpBytes.length);
    buffer.writeBytes(senderIpBytes);
    buffer.writeInt(address.port());
  }

  @Override
  protected void encodeMessage(ProtocolMessage message, ByteBuf buffer) {
    buffer.writeByte(message.type().id());
    writeLong(buffer, message.id());

    final byte[] payload = message.payload();
    writeInt(buffer, payload.length);
    buffer.writeBytes(payload);
  }

  @Override
  protected void encodeRequest(ProtocolRequest request, ByteBuf out) {
    writeString(out, request.subject());
  }

  @Override
  protected void encodeReply(ProtocolReply reply, ByteBuf out) {
    out.writeByte(reply.status().id());
  }
}
