/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.storage.journal;

import java.util.concurrent.locks.Lock;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Journal reader delegate.
 */
public class JournalReaderDelegate<E> implements JournalReader<E> {
  private final JournalReader<E> delegate;

  public JournalReaderDelegate(JournalReader<E> delegate) {
    this.delegate = delegate;
  }

  @Override
  public Lock lock() {
    return delegate.lock();
  }

  @Override
  public long currentIndex() {
    return delegate.currentIndex();
  }

  @Override
  public Indexed<E> currentEntry() {
    return delegate.currentEntry();
  }

  @Override
  public long nextIndex() {
    return delegate.nextIndex();
  }

  @Override
  public Indexed<E> get(long index) {
    return delegate.get(index);
  }

  @Override
  public Indexed<E> reset(long index) {
    return delegate.reset(index);
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public Indexed<E> next() {
    return delegate.next();
  }

  @Override
  public void reset() {
    delegate.reset();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("delegate", delegate)
        .toString();
  }
}
