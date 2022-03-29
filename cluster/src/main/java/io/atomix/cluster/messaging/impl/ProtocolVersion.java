// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import java.util.stream.Stream;

import io.atomix.utils.net.Address;

/**
 * Messaging protocol version.
 */
public enum ProtocolVersion {
  V1(1) {
    @Override
    public MessagingProtocol createProtocol(Address address) {
      return new MessagingProtocolV1(address);
    }
  },
  V2(2) {
    @Override
    public MessagingProtocol createProtocol(Address address) {
      return new MessagingProtocolV2(address);
    }
  };

  /**
   * Returns the protocol version for the given version number.
   *
   * @param version the version number for which to return the protocol version
   * @return the protocol version for the given version number
   */
  public static ProtocolVersion valueOf(int version) {
    return Stream.of(values())
        .filter(v -> v.version() == version)
        .findFirst()
        .orElse(null);
  }

  /**
   * Returns the latest protocol version.
   *
   * @return the latest protocol version
   */
  public static ProtocolVersion latest() {
    return values()[values().length - 1];
  }

  private final short version;

  ProtocolVersion(int version) {
    this.version = (short) version;
  }

  /**
   * Returns the version number.
   *
   * @return the version number
   */
  public short version() {
    return version;
  }

  /**
   * Creates a new protocol instance.
   *
   * @param address the protocol address
   * @return a new protocol instance
   */
  public abstract MessagingProtocol createProtocol(Address address);

}
