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
package net.kuujo.copycat.raft.storage;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.Writable;
import net.kuujo.copycat.io.util.ReferenceCounted;
import net.kuujo.copycat.io.util.ReferenceManager;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Raft entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class RaftEntry<T extends RaftEntry<T>> implements ReferenceCounted<RaftEntry>, Writable {
  private final ReferenceManager<RaftEntry<?>> referenceManager;
  private final AtomicInteger references = new AtomicInteger();
  private long index;
  private long term;

  protected RaftEntry(ReferenceManager<RaftEntry<?>> referenceManager) {
    this.referenceManager = referenceManager;
  }

  /**
   * Sets the entry index.
   *
   * @param index The entry index.
   */
  protected void setIndex(long index) {
    this.index = index;
  }

  /**
   * Returns the entry index.
   *
   * @return The entry index.
   */
  public long getIndex() {
    return index;
  }

  /**
   * Returns the entry term.
   *
   * @return The entry term.
   */
  public long getTerm() {
    return term;
  }

  /**
   * Sets the entry term.
   *
   * @param term The entry term.
   * @return The entry.
   */
  @SuppressWarnings("unchecked")
  public T setTerm(long term) {
    this.term = term;
    return (T) this;
  }

  /**
   * Returns the entry size.
   *
   * @return The entry size.
   */
  public int size() {
    return 8;
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeLong(term);
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    term = buffer.readLong();
  }

  @Override
  public RaftEntry acquire() {
    references.incrementAndGet();
    return this;
  }

  @Override
  public void release() {
    int refs = references.decrementAndGet();
    if (refs == 0) {
      referenceManager.release(this);
    } else if (refs < 0) {
      references.set(0);
    }
  }

  @Override
  public int references() {
    return references.get();
  }

  @Override
  public void close() {
    references.set(0);
    referenceManager.release(this);
  }

}
