/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.cluster.messaging.impl;

import java.io.IOException;

import io.atomix.utils.net.Address;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encode InternalMessage out into a byte buffer.
 */
abstract class AbstractMessageEncoder extends MessageToByteEncoder<Object> {
// Effectively MessageToByteEncoder<InternalMessage>,
// had to specify <Object> to avoid Class Loader not being able to find some classes.

  private final Logger log = LoggerFactory.getLogger(getClass());

  protected final Address address;
  private boolean addressWritten;

  AbstractMessageEncoder(Address address) {
    super();
    this.address = address;
  }

  @Override
  protected void encode(
      ChannelHandlerContext context,
      Object rawMessage,
      ByteBuf out) {
    if (!addressWritten) {
      encodeAddress((ProtocolMessage) rawMessage, out);
      addressWritten = true;
    }

    encodeMessage((ProtocolMessage) rawMessage, out);

    if (rawMessage instanceof ProtocolRequest) {
      encodeRequest((ProtocolRequest) rawMessage, out);
    } else if (rawMessage instanceof ProtocolReply) {
      encodeReply((ProtocolReply) rawMessage, out);
    }
  }

  protected abstract void encodeAddress(ProtocolMessage message, ByteBuf buffer);

  protected abstract void encodeMessage(ProtocolMessage message, ByteBuf buffer);

  protected abstract void encodeRequest(ProtocolRequest request, ByteBuf out);

  protected abstract void encodeReply(ProtocolReply reply, ByteBuf out);

  static void writeString(ByteBuf buffer, String value) {
    final ByteBuf buf = buffer.alloc().buffer(ByteBufUtil.utf8MaxBytes(value));
    try {
      final int length = ByteBufUtil.writeUtf8(buf, value);
      buffer.writeShort(length);
      buffer.writeBytes(buf);
    } finally {
      buf.release();
    }
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
