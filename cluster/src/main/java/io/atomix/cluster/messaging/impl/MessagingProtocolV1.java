// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import io.atomix.utils.net.Address;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * V1 messaging protocol.
 */
public class MessagingProtocolV1 implements MessagingProtocol {
  private final Address address;

  MessagingProtocolV1(Address address) {
    this.address = address;
  }

  @Override
  public ProtocolVersion version() {
    return ProtocolVersion.V1;
  }

  @Override
  public MessageToByteEncoder<Object> newEncoder() {
    return new MessageEncoderV1(address);
  }

  @Override
  public ByteToMessageDecoder newDecoder() {
    return new MessageDecoderV1();
  }
}
