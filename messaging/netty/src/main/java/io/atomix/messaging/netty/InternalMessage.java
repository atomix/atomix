/*
 * Copyright 2016-present Open Networking Laboratory
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

import io.atomix.messaging.Endpoint;
import io.atomix.utils.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Internal message representation with additional attributes
 * for supporting, synchronous request/reply behavior.
 */
public final class InternalMessage {

  /**
   * Message status.
   */
  public enum Status {

    // NOTE: For backwards compatibility enum constant IDs should not be changed.

    /**
     * All ok.
     */
    OK(0),

    /**
     * Response status signifying no registered handler.
     */
    ERROR_NO_HANDLER(1),

    /**
     * Response status signifying an exception handling the message.
     */
    ERROR_HANDLER_EXCEPTION(2),

    /**
     * Response status signifying invalid message structure.
     */
    PROTOCOL_EXCEPTION(3);

    private final int id;

    Status(int id) {
      this.id = id;
    }

    /**
     * Returns the unique status ID.
     *
     * @return the unique status ID.
     */
    public int id() {
      return id;
    }

    /**
     * Returns the status enum associated with the given ID.
     *
     * @param id the status ID.
     * @return the status enum for the given ID.
     */
    public static Status forId(int id) {
      switch (id) {
        case 0:
          return OK;
        case 1:
          return ERROR_NO_HANDLER;
        case 2:
          return ERROR_HANDLER_EXCEPTION;
        case 3:
          return PROTOCOL_EXCEPTION;
        default:
          throw new IllegalArgumentException("Unknown status ID " + id);
      }
    }
  }

  private final int preamble;
  private final long id;
  private final Endpoint sender;
  private final String type;
  private final byte[] payload;
  private final Status status;

  public InternalMessage(int preamble,
                         long id,
                         Endpoint sender,
                         String type,
                         byte[] payload) {
    this(preamble, id, sender, type, payload, null);
  }

  public InternalMessage(int preamble,
                         long id,
                         Endpoint sender,
                         byte[] payload,
                         Status status) {
    this(preamble, id, sender, "", payload, status);
  }

  InternalMessage(int preamble,
                  long id,
                  Endpoint sender,
                  String type,
                  byte[] payload,
                  Status status) {
    this.preamble = preamble;
    this.id = id;
    this.sender = sender;
    this.type = type;
    this.payload = payload;
    this.status = status;
  }

  public boolean isRequest() {
    return status == null;
  }

  public boolean isReply() {
    return status != null;
  }

  public int preamble() {
    return preamble;
  }

  public long id() {
    return id;
  }

  public String type() {
    return type;
  }

  public Endpoint sender() {
    return sender;
  }

  public byte[] payload() {
    return payload;
  }

  public Status status() {
    return status;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id)
        .add("type", type)
        .add("sender", sender)
        .add("status", status)
        .add("payload", ArraySizeHashPrinter.of(payload))
        .toString();
  }
}
