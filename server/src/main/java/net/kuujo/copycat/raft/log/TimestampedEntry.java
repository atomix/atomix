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
package net.kuujo.copycat.raft.log;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.alleycat.util.ReferenceManager;
import net.kuujo.copycat.log.Entry;

/**
 * Timestamped entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class TimestampedEntry<T extends TimestampedEntry<T>> extends RaftEntry<T> {
  private long timestamp;

  protected TimestampedEntry() {
  }

  protected TimestampedEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the entry timestamp.
   *
   * @return The entry timestamp.
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Sets the entry timestamp.
   *
   * @param timestamp The entry timestamp.
   * @return The entry.
   */
  @SuppressWarnings("unchecked")
  public T setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return (T) this;
  }

  @Override
  public int size() {
    return super.size() + Long.BYTES;
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    super.writeObject(buffer, alleycat);
    buffer.writeLong(timestamp);
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat alleycat) {
    super.readObject(buffer, alleycat);
    timestamp = buffer.readLong();
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, timestamp=%d]", getClass().getSimpleName(), getIndex(), getTerm(), timestamp);
  }

}
