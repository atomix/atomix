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

import io.atomix.cluster.NodeId;
import io.atomix.util.buffer.BufferInput;
import io.atomix.util.buffer.BufferOutput;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Open session entry.
 */
public class OpenSessionEntry extends TimestampedEntry<OpenSessionEntry> {
  private final NodeId node;
  private final String name;
  private final String type;
  private final long timeout;

  public OpenSessionEntry(long timestamp, NodeId node, String name, String type, long timeout) {
    super(timestamp);
    this.node = node;
    this.name = name;
    this.type = type;
    this.timeout = timeout;
  }

  @Override
  public Type<OpenSessionEntry> type() {
    return Type.OPEN_SESSION;
  }

  /**
   * Returns the client node identifier.
   *
   * @return The client node identifier.
   */
  public NodeId node() {
    return node;
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
    return toStringHelper(this)
        .add("timestamp", timestamp)
        .add("node", node)
        .add("name", name)
        .add("type", type)
        .add("timeout", timeout)
        .toString();
  }

  /**
   * Open session entry serializer.
   */
  public static class Serializer implements TimestampedEntry.Serializer<OpenSessionEntry> {
    @Override
    public void writeObject(BufferOutput output, OpenSessionEntry entry) {
      output.writeLong(entry.timestamp);
      output.writeString(entry.node.id());
      output.writeString(entry.name);
      output.writeString(entry.type);
      output.writeLong(entry.timeout);
    }

    @Override
    public OpenSessionEntry readObject(BufferInput input, Class<OpenSessionEntry> type) {
      return new OpenSessionEntry(input.readLong(), NodeId.nodeId(input.readString()), input.readString(), input.readString(), input.readLong());
    }
  }
}
