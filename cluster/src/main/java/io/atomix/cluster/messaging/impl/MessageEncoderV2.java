// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import io.atomix.utils.net.Address;
import io.netty.buffer.ByteBuf;

/**
 * V2 message encoder.
 */
class MessageEncoderV2 extends MessageEncoderV1 {
  MessageEncoderV2(Address address) {
    super(address);
  }

  @Override
  protected void encodeAddress(ProtocolMessage message, ByteBuf buffer) {
    writeString(buffer, address.host());
    buffer.writeInt(address.port());
  }
}
