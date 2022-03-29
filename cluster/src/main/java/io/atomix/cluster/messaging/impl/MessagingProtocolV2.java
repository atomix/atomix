// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import io.atomix.utils.net.Address;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * V2 messaging protocol.
 */
public class MessagingProtocolV2 implements MessagingProtocol {
  private final Address address;

  MessagingProtocolV2(Address address) {
    this.address = address;
  }

  @Override
  public ProtocolVersion version() {
    return ProtocolVersion.V2;
  }

  @Override
  public MessageToByteEncoder<Object> newEncoder() {
    return new MessageEncoderV2(address);
  }

  @Override
  public ByteToMessageDecoder newDecoder() {
    return new MessageDecoderV2();
  }
}
