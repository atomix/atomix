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
package net.kuujo.copycat.io.storage;

import net.kuujo.copycat.io.serializer.CopycatSerializable;
import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.ReferenceCounted;
import net.kuujo.copycat.util.ReferenceManager;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Log entry.
 * <p>
 * The {@code Entry} represents a single record in a Copycat {@link Log}. Each entry is stored at
 * a unique {@link #getIndex() index} in the log. Indexes are applied to entries once written to a log.
 * <p>
 * Custom entry implementations should implement serialization and deserialization logic via
 * {@link net.kuujo.copycat.io.serializer.CopycatSerializable#writeObject(net.kuujo.copycat.io.BufferOutput, net.kuujo.copycat.io.serializer.Serializer)}
 * and {@link net.kuujo.copycat.io.serializer.CopycatSerializable#readObject(net.kuujo.copycat.io.BufferInput, net.kuujo.copycat.io.serializer.Serializer)}.
 * respectively.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Entry<T extends Entry<T>> implements ReferenceCounted<Entry>, CopycatSerializable {
  private final ReferenceManager<Entry<?>> referenceManager;
  private final AtomicInteger references = new AtomicInteger();
  private long index;
  private int size = -1;

  protected Entry() {
    referenceManager = null;
  }

  /**
   * @throws NullPointerException if {@code referenceManager} is null
   */
  protected Entry(ReferenceManager<Entry<?>> referenceManager) {
    this.referenceManager = referenceManager;
  }

  /**
   * Resets the entry state.
   */
  @SuppressWarnings("unchecked")
  protected T reset() {
    this.index = 0;
    this.size = -1;
    return (T) this;
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
   * Sets the entry index.
   *
   * @param index The entry index.
   * @return The entry.
   */
  @SuppressWarnings("unchecked")
  public T setIndex(long index) {
    this.index = index;
    return (T) this;
  }

  /**
   * Returns the entry size.
   *
   * @return The entry size.
   * @throws IllegalStateException If the entry has not yet been persisted
   */
  public int size() {
    Assert.stateNot(size == -1, "cannot determine size for non-persisted entry");
    return size;
  }

  /**
   * Sets the entry size.
   *
   * @param size The entry size.
   * @return The entry.
   */
  @SuppressWarnings("unchecked")
  T setSize(int size) {
    this.size = size;
    return (T) this;
  }

  @Override
  public Entry acquire() {
    references.incrementAndGet();
    return this;
  }

  @Override
  public void release() {
    int refs = references.decrementAndGet();
    if (refs == 0) {
      if (referenceManager != null)
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
    if (referenceManager != null)
      referenceManager.release(this);
  }

}
