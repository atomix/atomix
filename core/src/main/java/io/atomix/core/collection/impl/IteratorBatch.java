/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.collection.impl;

import java.util.Collection;
import java.util.Iterator;

/**
 * Iterator batch.
 */
public final class IteratorBatch<T> implements Iterator<T> {
  private final int position;
  private final Collection<T> entries;
  private transient volatile Iterator<T> iterator;

  public IteratorBatch(int position, Collection<T> entries) {
    this.position = position;
    this.entries = entries;
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