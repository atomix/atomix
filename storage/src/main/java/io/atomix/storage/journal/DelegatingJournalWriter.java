// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Journal writer delegate.
 */
public class DelegatingJournalWriter<E> implements JournalWriter<E> {
  private final JournalWriter<E> delegate;

  public DelegatingJournalWriter(JournalWriter<E> delegate) {
    this.delegate = delegate;
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
  public <T extends E> Indexed<T> append(T entry) {
    return delegate.append(entry);
  }

  @Override
  public void append(Indexed<E> entry) {
    delegate.append(entry);
  }

  @Override
  public void commit(long index) {
    delegate.commit(index);
  }

  @Override
  public void reset(long index) {
    delegate.reset(index);
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
