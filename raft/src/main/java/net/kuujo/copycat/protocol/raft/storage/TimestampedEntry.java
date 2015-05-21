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
package net.kuujo.copycat.protocol.raft.storage;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.util.ReferenceManager;

/**
 * Timestamped entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class TimestampedEntry<T extends TimestampedEntry<T>> extends RaftEntry<T> {
  private long timestamp;

  protected TimestampedEntry(ReferenceManager<RaftEntry<?>> referenceManager) {
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
    return super.size() + 8;
  }

  @Override
  public void writeObject(Buffer buffer) {
    super.writeObject(buffer);
    buffer.writeLong(timestamp);
  }

  @Override
  public void readObject(Buffer buffer) {
    super.readObject(buffer);
    timestamp = buffer.readLong();
  }

}
