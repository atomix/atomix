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
 * Journal writer delegate.
 */
public class JournalWriterDelegate<E> implements JournalWriter<E> {
  private final JournalWriter<E> delegate;

  public JournalWriterDelegate(JournalWriter<E> delegate) {
    this.delegate = delegate;
  }

  @Override
  public Lock getLock() {
    return delegate.getLock();
  }

  @Override
  public long getLastIndex() {
    return delegate.getLastIndex();
  }

  @Override
  public Indexed<E> getLastEntry() {
    return delegate.getLastEntry();
  }

  @Override
  public long getNextIndex() {
    return delegate.getNextIndex();
  }

  @Override
  public <T extends E> Indexed<T> appendEntry(T entry) {
    return delegate.appendEntry(entry);
  }

  @Override
  public void appendEntry(Indexed<E> entry) {
    delegate.appendEntry(entry);
  }

  @Override
  public void truncate(long index) {
    delegate.truncate(index);
  }

  @Override
  public void flush() {
    delegate.flush();
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
