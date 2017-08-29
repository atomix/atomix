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
package io.atomix.messaging.netty;

import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.atomix.messaging.Endpoint;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.net.InetAddress;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Decoder for inbound messages.
 */
public class MessageDecoder extends ReplayingDecoder<DecoderState> {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private long messageId;
  private int preamble;
  private InetAddress senderIp;
  private int senderPort;
  private int messageTypeLength;
  private String messageType;
  private InternalMessage.Status status;
  private int contentLength;

  public MessageDecoder() {
    super(DecoderState.READ_MESSAGE_PREAMBLE);
  }

  @Override
  @SuppressWarnings("squid:S128") // suppress switch fall through warning
  protected void decode(
      ChannelHandlerContext context,
      ByteBuf buffer,
      List<Object> out) throws Exception {

    switch (state()) {
      case READ_MESSAGE_PREAMBLE:
        preamble = buffer.readInt();
        checkpoint(DecoderState.READ_MESSAGE_ID);
      case READ_MESSAGE_ID:
        messageId = buffer.readLong();
        checkpoint(DecoderState.READ_SENDER_IP);
      case READ_SENDER_IP:
        byte[] octets = new byte[buffer.readByte()];
        buffer.readBytes(octets);
        senderIp = InetAddress.getByAddress(octets);
        checkpoint(DecoderState.READ_SENDER_PORT);
      case READ_SENDER_PORT:
        senderPort = buffer.readInt();
        checkpoint(DecoderState.READ_MESSAGE_TYPE_LENGTH);
      case READ_MESSAGE_TYPE_LENGTH:
        messageTypeLength = buffer.readShort();
        checkpoint(DecoderState.READ_MESSAGE_TYPE);
      case READ_MESSAGE_TYPE:
        byte[] messageTypeBytes = new byte[messageTypeLength];
        buffer.readBytes(messageTypeBytes);
        messageType = new String(messageTypeBytes, Charsets.UTF_8);
        checkpoint(DecoderState.READ_MESSAGE_STATUS);
      case READ_MESSAGE_STATUS:
        int statusId = buffer.readByte();
        if (statusId == -1) {
          status = null;
        } else {
          status = InternalMessage.Status.forId(statusId);
        }
        checkpoint(DecoderState.READ_CONTENT_LENGTH);
      case READ_CONTENT_LENGTH:
        contentLength = buffer.readInt();
        checkpoint(DecoderState.READ_CONTENT);
      case READ_CONTENT:
        byte[] payload;
        if (contentLength > 0) {
          //TODO Perform a sanity check on the size before allocating
          payload = new byte[contentLength];
          buffer.readBytes(payload);
        } else {
          payload = new byte[0];
        }
        InternalMessage message = new InternalMessage(preamble,
            messageId,
            new Endpoint(senderIp, senderPort),
            messageType,
            payload,
            status);
        out.add(message);
        checkpoint(DecoderState.READ_MESSAGE_PREAMBLE);
        break;
      default:
        checkState(false, "Must not be here");
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
    log.error("Exception inside channel handling pipeline.", cause);
    context.close();
  }
}
