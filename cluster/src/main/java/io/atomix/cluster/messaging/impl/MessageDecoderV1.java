// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import java.net.InetAddress;
import java.util.List;

import io.atomix.utils.net.Address;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import static com.google.common.base.Preconditions.checkState;

/**
 * Decoder for inbound messages.
 */
class MessageDecoderV1 extends AbstractMessageDecoder {

  /**
   * V1 decoder state.
   */
  enum DecoderState {
    READ_TYPE,
    READ_MESSAGE_ID,
    READ_SENDER_IP,
    READ_SENDER_PORT,
    READ_SUBJECT_LENGTH,
    READ_SUBJECT,
    READ_STATUS,
    READ_CONTENT_LENGTH,
    READ_CONTENT
  }

  private DecoderState currentState = DecoderState.READ_SENDER_IP;

  private InetAddress senderIp;
  private int senderPort;
  private Address senderAddress;

  private ProtocolMessage.Type type;
  private long messageId;
  private int contentLength;
  private byte[] content;
  private int subjectLength;

  @Override
  @SuppressWarnings("squid:S128") // suppress switch fall through warning
  protected void decode(
      ChannelHandlerContext context,
      ByteBuf buffer,
      List<Object> out) throws Exception {

    switch (currentState) {
      case READ_SENDER_IP:
        if (buffer.readableBytes() < Byte.BYTES) {
          return;
        }
        buffer.markReaderIndex();
        int octetsLength = buffer.readByte();
        if (buffer.readableBytes() < octetsLength) {
          buffer.resetReaderIndex();
          return;
        }

        byte[] octets = new byte[octetsLength];
        buffer.readBytes(octets);
        senderIp = InetAddress.getByAddress(octets);
        currentState = DecoderState.READ_SENDER_PORT;
      case READ_SENDER_PORT:
        if (buffer.readableBytes() < Integer.BYTES) {
          return;
        }
        senderPort = buffer.readInt();
        senderAddress = new Address(senderIp.getHostName(), senderPort, senderIp);
        currentState = DecoderState.READ_TYPE;
      case READ_TYPE:
        if (buffer.readableBytes() < Byte.BYTES) {
          return;
        }
        type = ProtocolMessage.Type.forId(buffer.readByte());
        currentState = DecoderState.READ_MESSAGE_ID;
      case READ_MESSAGE_ID:
        try {
          messageId = readLong(buffer);
        } catch (Escape e) {
          return;
        }
        currentState = DecoderState.READ_CONTENT_LENGTH;
      case READ_CONTENT_LENGTH:
        try {
          contentLength = readInt(buffer);
        } catch (Escape e) {
          return;
        }
        currentState = DecoderState.READ_CONTENT;
      case READ_CONTENT:
        if (buffer.readableBytes() < contentLength) {
          return;
        }
        if (contentLength > 0) {
          // TODO: Perform a sanity check on the size before allocating
          content = new byte[contentLength];
          buffer.readBytes(content);
        } else {
          content = EMPTY_PAYLOAD;
        }

        switch (type) {
          case REQUEST:
            currentState = DecoderState.READ_SUBJECT_LENGTH;
            break;
          case REPLY:
            currentState = DecoderState.READ_STATUS;
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
        switch (currentState) {
          case READ_SUBJECT_LENGTH:
            if (buffer.readableBytes() < Short.BYTES) {
              return;
            }
            subjectLength = buffer.readShort();
            currentState = DecoderState.READ_SUBJECT;
          case READ_SUBJECT:
            if (buffer.readableBytes() < subjectLength) {
              return;
            }
            final String subject = readString(buffer, subjectLength);
            ProtocolRequest message = new ProtocolRequest(messageId, senderAddress, subject, content);
            out.add(message);
            currentState = DecoderState.READ_TYPE;
            break;
          default:
            break;
        }
        break;
      case REPLY:
        switch (currentState) {
          case READ_STATUS:
            if (buffer.readableBytes() < Byte.BYTES) {
              return;
            }
            ProtocolReply.Status status = ProtocolReply.Status.forId(buffer.readByte());
            ProtocolReply message = new ProtocolReply(messageId, content, status);
            out.add(message);
            currentState = DecoderState.READ_TYPE;
            break;
          default:
            break;
        }
        break;
      default:
        checkState(false, "Must not be here");
    }
  }
}
