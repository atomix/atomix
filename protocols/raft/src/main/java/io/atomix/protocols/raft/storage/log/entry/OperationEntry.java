/*
 * Copyright 2015-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.storage.log.entry;

import io.atomix.utils.ArraySizeHashPrinter;

import java.util.Date;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Stores a state machine operation.
 * <p>
 * Each state machine operation is stored with a client-provided {@link #getSequenceNumber() sequence number}.
 * The sequence number is used by state machines to apply client operations in the order in which they
 * were submitted by the client (FIFO order). Additionally, each operation is written with the leader's
 * {@link #getTimestamp() timestamp} at the time the entry was logged. This gives state machines an
 * approximation of time with which to react to the application of operations to the state machine.
 */
public abstract class OperationEntry extends SessionEntry {
  protected final long sequence;
  protected final byte[] bytes;

  public OperationEntry(long term, long timestamp, long session, long sequence, byte[] bytes) {
    super(term, timestamp, session);
    this.sequence = sequence;
    this.bytes = bytes;
  }

  /**
   * Returns the entry operation bytes.
   *
   * @return The entry operation bytes.
   */
  public byte[] getBytes() {
    return bytes;
  }

  /**
   * Returns the operation sequence number.
   *
   * @return The operation sequence number.
   */
  public long getSequenceNumber() {
    return sequence;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("timestamp", new Date(timestamp))
        .add("session", session)
        .add("sequence", sequence)
        .add("operation", ArraySizeHashPrinter.of(bytes))
        .toString();
  }
}
