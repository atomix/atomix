// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

/**
 * Base class for internal messages.
 */
public abstract class ProtocolMessage {

  /**
   * Internal message type.
   */
  public enum Type {
    REQUEST(1),
    REPLY(2);

    private final int id;

    Type(int id) {
      this.id = id;
    }

    /**
     * Returns the unique message type ID.
     *
     * @return the unique message type ID.
     */
    public int id() {
      return id;
    }

    /**
     * Returns the message type enum associated with the given ID.
     *
     * @param id the type ID.
     * @return the type enum for the given ID.
     */
    public static Type forId(int id) {
      switch (id) {
        case 1:
          return REQUEST;
        case 2:
          return REPLY;
        default:
          throw new IllegalArgumentException("Unknown status ID " + id);
      }
    }
  }

  private final long id;
  private final byte[] payload;

  protected ProtocolMessage(long id, byte[] payload) {
    this.id = id;
    this.payload = payload;
  }

  public abstract Type type();

  public boolean isRequest() {
    return type() == Type.REQUEST;
  }

  public boolean isReply() {
    return type() == Type.REPLY;
  }

  public long id() {
    return id;
  }

  public byte[] payload() {
    return payload;
  }
}
