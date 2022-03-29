// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Messaging protocol.
 */
public interface MessagingProtocol {

  /**
   * Returns the protocol version.
   *
   * @return the protocol version
   */
  ProtocolVersion version();

  /**
   * Returns a new message encoder.
   *
   * @return a new message encoder
   */
  MessageToByteEncoder<Object> newEncoder();

  /**
   * Returns a new message decoder.
   *
   * @return a new message decoder
   */
  ByteToMessageDecoder newDecoder();

}
