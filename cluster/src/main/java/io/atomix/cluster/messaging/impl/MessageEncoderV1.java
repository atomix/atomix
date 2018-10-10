/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.cluster.messaging.impl;

import io.atomix.utils.net.Address;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Encode InternalMessage out into a byte buffer.
 */
class MessageEncoderV1 extends MessageToByteEncoder<Object> {
// Effectively MessageToByteEncoder<InternalMessage>,
// had to specify <Object> to avoid Class Loader not being able to find some classes.

  private final Logger log = LoggerFactory.getLogger(getClass());

  private final Address address;
  private boolean addressWritten;

  MessageEncoderV1(Address address) {
    super();
    this.address = address;
  }

  @Override
  protected void encode(
      ChannelHandlerContext context,
      Object rawMessage,
      ByteBuf out) throws Exception {
    if (rawMessage instanceof ProtocolRequest) {
      encodeRequest((ProtocolRequest) rawMessage, out);
    } else if (rawMessage instanceof ProtocolReply) {
      encodeReply((ProtocolReply) rawMessage, out);
    }
  }

  private void encodeMessage(ProtocolMessage message, ByteBuf buffer) {
    // If the address hasn't been written to the channel, write it.
    if (!addressWritten) {
      final InetAddress senderIp = address.address();
      final byte[] senderIpBytes = senderIp.getAddress();
      buffer.writeByte(senderIpBytes.length);
      buffer.writeBytes(senderIpBytes);

      // write sender port
      buffer.writeInt(address.port());

      addressWritten = true;
    }

    // Write the message type ID
    buffer.writeByte(message.type().id());

    // Write the message ID as a variable length integer
    writeLong(buffer, message.id());

    final byte[] payload = message.payload();

    // Write the payload length as a variable length integer
    writeInt(buffer, payload.length);

    // Write the payload bytes
    buffer.writeBytes(payload);
  }

  private void encodeRequest(ProtocolRequest request, ByteBuf out) {
    encodeMessage(request, out);

    final ByteBuf buf = out.alloc().buffer(ByteBufUtil.utf8MaxBytes(request.subject()));
    try {
      final int length = ByteBufUtil.writeUtf8(buf, request.subject());
      // write length of message type
      out.writeShort(length);
      // write message type bytes
      out.writeBytes(buf);
    } finally {
      buf.release();
    }
  }

  private void encodeReply(ProtocolReply reply, ByteBuf out) {
    encodeMessage(reply, out);

    // write message status value
    out.writeByte(reply.status().id());
  }

  static void writeInt(ByteBuf buf, int value) {
    if (value >>> 7 == 0) {
      buf.writeByte(value);
    } else if (value >>> 14 == 0) {
      buf.writeByte((value & 0x7F) | 0x80);
      buf.writeByte(value >>> 7);
    } else if (value >>> 21 == 0) {
      buf.writeByte((value & 0x7F) | 0x80);
      buf.writeByte(value >>> 7 | 0x80);
      buf.writeByte(value >>> 14);
    } else if (value >>> 28 == 0) {
      buf.writeByte((value & 0x7F) | 0x80);
      buf.writeByte(value >>> 7 | 0x80);
      buf.writeByte(value >>> 14 | 0x80);
      buf.writeByte(value >>> 21);
    } else {
      buf.writeByte((value & 0x7F) | 0x80);
      buf.writeByte(value >>> 7 | 0x80);
      buf.writeByte(value >>> 14 | 0x80);
      buf.writeByte(value >>> 21 | 0x80);
      buf.writeByte(value >>> 28);
    }
  }

  static void writeLong(ByteBuf buf, long value) {
    if (value >>> 7 == 0) {
      buf.writeByte((byte) value);
    } else if (value >>> 14 == 0) {
      buf.writeByte((byte) ((value & 0x7F) | 0x80));
      buf.writeByte((byte) (value >>> 7));
    } else if (value >>> 21 == 0) {
      buf.writeByte((byte) ((value & 0x7F) | 0x80));
      buf.writeByte((byte) (value >>> 7 | 0x80));
      buf.writeByte((byte) (value >>> 14));
    } else if (value >>> 28 == 0) {
      buf.writeByte((byte) ((value & 0x7F) | 0x80));
      buf.writeByte((byte) (value >>> 7 | 0x80));
      buf.writeByte((byte) (value >>> 14 | 0x80));
      buf.writeByte((byte) (value >>> 21));
    } else if (value >>> 35 == 0) {
      buf.writeByte((byte) ((value & 0x7F) | 0x80));
      buf.writeByte((byte) (value >>> 7 | 0x80));
      buf.writeByte((byte) (value >>> 14 | 0x80));
      buf.writeByte((byte) (value >>> 21 | 0x80));
      buf.writeByte((byte) (value >>> 28));
    } else if (value >>> 42 == 0) {
      buf.writeByte((byte) ((value & 0x7F) | 0x80));
      buf.writeByte((byte) (value >>> 7 | 0x80));
      buf.writeByte((byte) (value >>> 14 | 0x80));
      buf.writeByte((byte) (value >>> 21 | 0x80));
      buf.writeByte((byte) (value >>> 28 | 0x80));
      buf.writeByte((byte) (value >>> 35));
    } else if (value >>> 49 == 0) {
      buf.writeByte((byte) ((value & 0x7F) | 0x80));
      buf.writeByte((byte) (value >>> 7 | 0x80));
      buf.writeByte((byte) (value >>> 14 | 0x80));
      buf.writeByte((byte) (value >>> 21 | 0x80));
      buf.writeByte((byte) (value >>> 28 | 0x80));
      buf.writeByte((byte) (value >>> 35 | 0x80));
      buf.writeByte((byte) (value >>> 42));
    } else if (value >>> 56 == 0) {
      buf.writeByte((byte) ((value & 0x7F) | 0x80));
      buf.writeByte((byte) (value >>> 7 | 0x80));
      buf.writeByte((byte) (value >>> 14 | 0x80));
      buf.writeByte((byte) (value >>> 21 | 0x80));
      buf.writeByte((byte) (value >>> 28 | 0x80));
      buf.writeByte((byte) (value >>> 35 | 0x80));
      buf.writeByte((byte) (value >>> 42 | 0x80));
      buf.writeByte((byte) (value >>> 49));
    } else {
      buf.writeByte((byte) ((value & 0x7F) | 0x80));
      buf.writeByte((byte) (value >>> 7 | 0x80));
      buf.writeByte((byte) (value >>> 14 | 0x80));
      buf.writeByte((byte) (value >>> 21 | 0x80));
      buf.writeByte((byte) (value >>> 28 | 0x80));
      buf.writeByte((byte) (value >>> 35 | 0x80));
      buf.writeByte((byte) (value >>> 42 | 0x80));
      buf.writeByte((byte) (value >>> 49 | 0x80));
      buf.writeByte((byte) (value >>> 56));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
    try {
      if (cause instanceof IOException) {
        log.debug("IOException inside channel handling pipeline.", cause);
      } else {
        log.error("non-IOException inside channel handling pipeline.", cause);
      }
    } finally {
      context.close();
    }
  }

  // Effectively same result as one generated by MessageToByteEncoder<InternalMessage>
  @Override
  public final boolean acceptOutboundMessage(Object msg) throws Exception {
    return msg instanceof ProtocolMessage;
  }
}