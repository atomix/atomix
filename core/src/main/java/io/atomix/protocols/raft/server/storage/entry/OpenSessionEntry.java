/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.server.storage.entry;

import io.atomix.util.buffer.BufferInput;
import io.atomix.util.buffer.BufferOutput;

/**
 * Open session entry.
 */
public class OpenSessionEntry extends TimestampedEntry<OpenSessionEntry> {
  private final String client;
  private final String name;
  private final String type;
  private final long timeout;

  public OpenSessionEntry(long timestamp, String client, String name, String type, long timeout) {
    super(timestamp);
    this.client = client;
    this.name = name;
    this.type = type;
    this.timeout = timeout;
  }

  @Override
  public Type<OpenSessionEntry> type() {
    return Type.OPEN_SESSION;
  }

  /**
   * Returns the client identifier.
   *
   * @return The client identifier.
   */
  public String client() {
    return client;
  }

  /**
   * Returns the session state machine name.
   *
   * @return The session's state machine name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the session state machine type name.
   *
   * @return The session's state machine type name.
   */
  public String typeName() {
    return type;
  }

  /**
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  public long timeout() {
    return timeout;
  }

  @Override
  public String toString() {
    return String.format("%s[client=%s, name=%s, type=%s, timeout=%d, timestamp=%d]", getClass().getSimpleName(), client, name, type, timeout, timestamp);
  }

  /**
   * Open session entry serializer.
   */
  public static class Serializer implements TimestampedEntry.Serializer<OpenSessionEntry> {
    @Override
    public void writeObject(BufferOutput output, OpenSessionEntry entry) {
      output.writeLong(entry.timestamp);
      output.writeString(entry.client);
      output.writeString(entry.name);
      output.writeString(entry.type);
      output.writeLong(entry.timeout);
    }

    @Override
    public OpenSessionEntry readObject(BufferInput input, Class<OpenSessionEntry> type) {
      return new OpenSessionEntry(input.readLong(), input.readString(), input.readString(), input.readString(), input.readLong());
    }
  }
}
