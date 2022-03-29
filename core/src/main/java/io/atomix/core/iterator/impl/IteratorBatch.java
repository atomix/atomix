// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.iterator.impl;

import java.util.Collection;
import java.util.Iterator;

/**
 * Iterator batch.
 */
public final class IteratorBatch<T> implements Iterator<T> {
  private final long id;
  private final int position;
  private final Collection<T> entries;
  private final boolean complete;
  private transient volatile Iterator<T> iterator;

  public IteratorBatch(long id, int position, Collection<T> entries, boolean complete) {
    this.id = id;
    this.position = position;
    this.entries = entries;
    this.complete = complete;
  }

  /**
   * Returns the iterator identifier.
   *
   * @return the iterator identifier
   */
  public long id() {
    return id;
  }

  /**
   * Returns the iterator position.
   *
   * @return the iterator position
   */
  public int position() {
    return position;
  }

  /**
   * Returns the batch of entries.
   *
   * @return the batch of entries
   */
  public Collection<T> entries() {
    return entries;
  }

  /**
   * Returns a boolean indicating whether the batch is complete.
   *
   * @return indicates whether this batch completes iteration
   */
  public boolean complete() {
    return complete;
  }

  private Iterator<T> iterator() {
    if (iterator == null) {
      synchronized (this) {
        if (iterator == null) {
          iterator = entries.iterator();
        }
      }
    }
    return iterator;
  }

  @Override
  public boolean hasNext() {
    return iterator().hasNext();
  }

  @Override
  public T next() {
    return iterator().next();
  }
}
