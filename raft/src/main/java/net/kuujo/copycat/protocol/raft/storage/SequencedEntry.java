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
 * Sequenced entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SequencedEntry<T extends SequencedEntry<T>> extends MemberEntry<T> {
  private long sequenceNumber;

  public SequencedEntry(ReferenceManager<RaftEntry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the entry sequence number.
   *
   * @return The entry sequence number.
   */
  public long getSequenceNumber() {
    return sequenceNumber;
  }

  /**
   * Sets the entry sequence number.
   *
   * @param sequenceNumber The entry sequence number.
   * @return The entry.
   */
  @SuppressWarnings("unchecked")
  public T setSequenceNumber(long sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
    return (T) this;
  }

  @Override
  public int size() {
    return super.size() + 8;
  }

  @Override
  public void writeObject(Buffer buffer) {
    super.writeObject(buffer);
    buffer.writeLong(sequenceNumber);
  }

  @Override
  public void readObject(Buffer buffer) {
    super.readObject(buffer);
    sequenceNumber = buffer.readLong();
  }

}
