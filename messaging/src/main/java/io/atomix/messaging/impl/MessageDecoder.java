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
package io.atomix.messaging.impl;

import io.atomix.messaging.Endpoint;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Decoder for inbound messages.
 */
public class MessageDecoder extends ReplayingDecoder<DecoderState> {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private static final byte[] EMPTY_PAYLOAD = new byte[0];

  private InetAddress senderIp;
  private int senderPort;

  private InternalMessage.Type type;
  private int preamble;
  private long messageId;
  private int contentLength;
  private byte[] content;
  private int subjectLength;
  private String subject;
  private InternalReply.Status status;

  public MessageDecoder() {
    super(DecoderState.READ_SENDER_IP);
  }

  @Override
  @SuppressWarnings("squid:S128") // suppress switch fall through warning
  protected void decode(
      ChannelHandlerContext context,
      ByteBuf buffer,
      List<Object> out) throws Exception {

    switch (state()) {
      case READ_SENDER_IP:
        byte[] octets = new byte[buffer.readByte()];
        buffer.readBytes(octets);
        senderIp = InetAddress.getByAddress(octets);
        checkpoint(DecoderState.READ_SENDER_PORT);
      case READ_SENDER_PORT:
        senderPort = buffer.readInt();
        checkpoint(DecoderState.READ_TYPE);
      case READ_TYPE:
        type = InternalMessage.Type.forId(buffer.readByte());
        checkpoint(DecoderState.READ_PREAMBLE);
      case READ_PREAMBLE:
        preamble = buffer.readInt();
        checkpoint(DecoderState.READ_MESSAGE_ID);
      case READ_MESSAGE_ID:
        messageId = buffer.readLong();
        checkpoint(DecoderState.READ_CONTENT_LENGTH);
      case READ_CONTENT_LENGTH:
        contentLength = buffer.readInt();
        checkpoint(DecoderState.READ_CONTENT);
      case READ_CONTENT:
        if (contentLength > 0) {
          //TODO Perform a sanity check on the size before allocating
          content = new byte[contentLength];
          buffer.readBytes(content);
        } else {
          content = EMPTY_PAYLOAD;
        }

        switch (type) {
          case REQUEST:
            checkpoint(DecoderState.READ_SUBJECT_LENGTH);
            break;
          case REPLY:
            checkpoint(DecoderState.READ_STATUS);
            break;
          default:
            checkState(false, "Must not be here");
        }
        break;
      default:
        break;
    }

    switch (type) {
      case REQUEST:
        switch (state()) {
          case READ_SUBJECT_LENGTH:
            subjectLength = buffer.readShort();
            checkpoint(DecoderState.READ_SUBJECT);
          case READ_SUBJECT:
            byte[] messageTypeBytes = new byte[subjectLength];
            buffer.readBytes(messageTypeBytes);
            subject = new String(messageTypeBytes, StandardCharsets.UTF_8);
            InternalRequest message = new InternalRequest(
                preamble,
                messageId,
                new Endpoint(senderIp, senderPort),
                subject,
                content);
            out.add(message);
            checkpoint(DecoderState.READ_TYPE);
            break;
          default:
            break;
        }
        break;
      case REPLY:
        switch (state()) {
          case READ_STATUS:
            status = InternalReply.Status.forId(buffer.readByte());
            InternalReply message = new InternalReply(preamble,
                messageId,
                content,
                status);
            out.add(message);
            checkpoint(DecoderState.READ_TYPE);
            break;
          default:
            break;
        }
        break;
      default:
        checkState(false, "Must not be here");
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
    try {
      log.error("Exception inside channel handling pipeline.", cause);
    } finally {
      context.close();
    }
  }
}