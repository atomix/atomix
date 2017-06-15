/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.protocols.raft.storage.entry;

import io.atomix.util.buffer.BufferInput;
import io.atomix.util.buffer.BufferOutput;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Indicates a leader change has occurred.
 * <p>
 * The {@code InitializeEntry} is logged by a leader at the beginning of its term to indicate that
 * a leadership change has occurred. Importantly, initialize entries are logged with a {@link #timestamp() timestamp}
 * which can be used by server state machines to reset session timeouts following leader changes. Initialize entries
 * are always the first entry to be committed at the start of a leader's term.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class InitializeEntry extends TimestampedEntry<InitializeEntry> {
  public InitializeEntry(long timestamp) {
    super(timestamp);
  }

  @Override
  public Type<InitializeEntry> type() {
    return Type.INITIALIZE;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("timestamp", timestamp)
        .toString();
  }

  /**
   * Initialize entry serializer.
   */
  public static class Serializer implements TimestampedEntry.Serializer<InitializeEntry> {
    @Override
    public void writeObject(BufferOutput output, InitializeEntry entry) {
      output.writeLong(entry.timestamp);
    }

    @Override
    public InitializeEntry readObject(BufferInput input, Class<InitializeEntry> type) {
      return new InitializeEntry(input.readLong());
    }
  }
}